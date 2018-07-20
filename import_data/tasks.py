import logging
import os.path
from datetime import timedelta, datetime

import requests
import configparser
import invoke
from invoke import task
from enum import Enum
from pydantic import BaseModel


logging.basicConfig(level=logging.INFO)


@task
def get_osm_data(ctx):
    """
    download the osm file and store it in the input_data directory
    """
    logging.info("downloading osm file from {}", ctx.osm.url)
    file_name = os.path.basename(ctx.osm.url)
    ctx.run(f"wget --progress=dot:giga {ctx.osm.url} --directory-prefix={ctx.data_dir}")
    new_osm_file = f"{ctx.data_dir}/{file_name}"
    if ctx.osm.file is not None and ctx.osm.file != new_osm_file:
        logging.warn(
            "the osm variable has been configured to {ctx.osm_file},"
            "but this will not be taken into account as we will use a newly downloaded file: {new_osm_file}"
        )
    ctx.osm.file = new_osm_file


@task
def cleanup_db_backup(ctx):
    """
    If there is a backup schema imposm cannot delete the tables in to 
    (with the -deployproduction), so we delete them to be able to reload the data several times
    """
    remove_backup = "DROP SCHEMA IF EXISTS backup CASCADE;"
    ctx.run(
        f'psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.database} -c "{remove_backup}"',
        env={"PGPASSWORD": ctx.pg.password},
    )


@task
def load_basemap(ctx):
    ctx.run(
        f'time imposm3 \
  import \
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.database}" \
  -read {ctx.osm.file} \
  -diff \
  -mapping {ctx.main_dir}/generated_mapping_base.yaml \
  -deployproduction -overwritecache \
  -optimize \
  -diffdir {ctx.generated_files_dir}/diff/base -cachedir {ctx.generated_files_dir}/cache/base'
    )


@task
def load_poi(ctx):
    ctx.run(
        f'time imposm3 \
  import \
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.database}" \
  -read {ctx.osm.file} \
  -diff \
  -mapping {ctx.main_dir}/generated_mapping_poi.yaml \
  -deployproduction -overwritecache \
  -optimize \
  -diffdir {ctx.generated_files_dir}/diff/poi -cachedir {ctx.generated_files_dir}/cache/poi'
    )


def _run_sql_script(ctx, script_name):
    ctx.run(
        f"psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.database} --set ON_ERROR_STOP='1' -f {ctx.sql_dir}/{script_name}",
        env={"PGPASSWORD": ctx.pg.password},
    )


@task
def run_sql_script(ctx):
    # load several psql functions
    _run_sql_script(ctx, "language.sql")
    _run_sql_script(ctx, "postgis-vt-util.sql")


@task
def import_natural_earth(ctx):
    logging.info("importing natural earth shapes in postgres")
    target_file = f"{ctx.data_dir}/natural_earth_vector.sqlite"

    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga http://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip \
        && unzip -oj natural_earth_vector.sqlite.zip -d {ctx.data_dir} \
        && rm natural_earth_vector.sqlite.zip"
        )

    pg_conn = f"dbname={ctx.pg.database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}"
    ctx.run(
        f'PGCLIENTENCODING=LATIN1 ogr2ogr \
    -progress \
    -f Postgresql \
    -s_srs EPSG:4326 \
    -t_srs EPSG:3857 \
    -clipsrc -180.1 -85.0511 180.1 85.0511 \
    PG:"{pg_conn}" \
    -lco GEOMETRY_NAME=geometry \
    -lco DIM=2 \
    -nlt GEOMETRY \
    -overwrite \
    {ctx.data_dir}/natural_earth_vector.sqlite'
    )


@task
def import_water_polygon(ctx):
    logging.info("importing water polygon shapes in postgres")

    target_file = f"{ctx.data_dir}/water_polygons.shp"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga http://data.openstreetmapdata.com/water-polygons-split-3857.zip \
    && unzip -oj water-polygons-split-3857.zip -d {ctx.data_dir} \
    && rm water-polygons-split-3857.zip"
        )

    ctx.run(
        f"POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DATA_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import-water.sh"
    )


@task
def import_lake(ctx):
    logging.info("importing the lakes borders in postgres")

    target_file = f"{ctx.data_dir}/lake_centerline.geojson"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga -L -P {ctx.data_dir} https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.geojson"
        )

    pg_conn = f"dbname={ctx.pg.database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}"
    ctx.run(
        f'PGCLIENTENCODING=UTF8 ogr2ogr \
    -f Postgresql \
    -s_srs EPSG:4326 \
    -t_srs EPSG:3857 \
    PG:"{pg_conn}" \
    {ctx.data_dir}/lake_centerline.geojson \
    -overwrite \
    -nln "lake_centerline"'
    )


@task
def import_border(ctx):
    logging.info("importing the borders in postgres")

    target_file = f"{ctx.data_dir}/osmborder_lines.csv"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga -P {ctx.data_dir} https://github.com/openmaptiles/import-osmborder/releases/download/v0.4/osmborder_lines.csv.gz \
    && gzip -d {ctx.data_dir}/osmborder_lines.csv.gz"
        )

    ctx.run(
        f"POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import_osmborder_lines.sh"
    )


@task
def import_wikidata(ctx):
    """
    import wikidata (for some translations)

    For the moment this does nothing (but we need a table for some openmaptiles function)
    """
    create_table = "CREATE TABLE IF NOT EXISTS wd_names (id varchar(20) UNIQUE, page varchar(200) UNIQUE, labels hstore);"
    ctx.run(
        f'psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.database} -c "{create_table}"',
        env={"PGPASSWORD": ctx.pg.password},
    )


@task
def run_post_sql_scripts(ctx):
    """
    load the sql file with all the functions to generate the layers
    this file has been generated using https://github.com/QwantResearch/openmaptiles
    """
    logging.info("running postsql scripts")
    _run_sql_script(ctx, "generated_base.sql")
    _run_sql_script(ctx, "generated_poi.sql")


@task
def load_osm(ctx):
    if ctx.osm.url:
        get_osm_data(ctx)
    cleanup_db_backup(ctx)
    load_basemap(ctx)
    load_poi(ctx)
    run_sql_script(ctx)


@task
def load_additional_data(ctx):
    import_natural_earth(ctx)
    import_water_polygon(ctx)
    import_lake(ctx)
    import_border(ctx)
    import_wikidata(ctx)


@task
def generate_tiles(ctx):
    """
    Start the tiles generation

    the Tiles generation process is handle in the background by tilerator
    """
    TilesLayer = Enum("TilesLayer", "BASEMAP POI")

    def generate_tiles(
        tiles_layer,
        from_zoom,
        before_zoom,
        z,
        x=None,
        y=None,
        check_previous_layer=False,
        check_base_layer_level=None,
    ):
        params = {
            "fromZoom": from_zoom,
            "beforeZoom": before_zoom,
            "keepJob": "true",
            "parts": ctx.tiles.parts,
            "deleteEmpty": "true",
            "zoom": z,
        }
        if tiles_layer == TilesLayer.BASEMAP:
            params.update(
                {
                    "generatorId": ctx.tiles.base_sources.generator,
                    "storageId": ctx.tiles.base_sources.storage,
                }
            )
        elif tiles_layer == TilesLayer.POI:
            params.update(
                {
                    "generatorId": ctx.tiles.poi_sources.generator,
                    "storageId": ctx.tiles.poi_sources.storage,
                }
            )
        else:
            raise Exception("invalid tiles_layer")

        if x:
            params["x"] = x
        if y:
            params["y"] = y
        if check_previous_layer:
            # this tells tilerator not to generate a tile if there is not tile at the previous zoom
            # this saves a lots of time since we won't generate tiles on oceans
            params["checkZoom"] = -1
        if check_base_layer_level:
            # this tells tilerator not to generate a tile if there is not tile at the previous zoom
            # this saves a lots of time since we won't generate tiles on oceans
            params["checkZoom"] = check_base_layer_level
            params["sourceId"] = ctx.tiles.base_sources.storage

        url = ctx.tiles.tilerator_host
        if ctx.tiles.tilerator_port:
            url += f":{ctx.tiles.tilerator_port}"
        url += "/add"

        logging.info(f"posting a tilerator job on {url} with params: {params}")
        res = requests.post(url, params=params)

        res.raise_for_status()
        json_res = res.json()
        if "error" in json_res:
            # tilerator can return status 200 but an error inside the response, so we need to check it
            raise Exception(
                f"impossible to run tilerator job, error: {json_res['error']}"
            )
        logging.info(f"jobs: {res.json()}")

    if ctx.tiles.planet:
        logging.info("generating tiles for the planet")
        # for the planet we tweak the tiles generation a bit to speed it up
        # we first generate all the tiles for the first levels
        generate_tiles(tiles_layer=TilesLayer.BASEMAP, z=0, from_zoom=0, before_zoom=10)
        # from the zoom 10 we generate only the tiles if there is a parent tiles
        # since tilerator does not generate tiles if the parent tile is composed only of 1 element
        # it speed up greatly the tiles generation by not even trying to generate tiles for oceans (and desert)
        generate_tiles(
            tiles_layer=TilesLayer.BASEMAP,
            z=10,
            from_zoom=10,
            before_zoom=15,
            check_previous_layer=True,
        )
        # for the poi, we generate only tiles if we have a base tile on the level 13
        # Note: we check the level 13 and not 14 because the tilegeneration process is in the background
        # and we might not have finished all basemap 14th zoom level tiles when starting the poi generation
        # it's a bit of a trick but works fine
        generate_tiles(
            tiles_layer=TilesLayer.POI,
            z=14,
            from_zoom=14,
            before_zoom=15,
            check_base_layer_level=13,
        )
    elif ctx.tiles.x and ctx.tiles.y and ctx.tiles.z:
        logging.info(
            f"generating tiles for {ctx.tiles.x} / {ctx.tiles.y}, z = {ctx.tiles.z}"
        )
        generate_tiles(
            tiles_layer=TilesLayer.BASEMAP,
            x=ctx.tiles.x,
            y=ctx.tiles.y,
            z=ctx.tiles.z,
            from_zoom=ctx.tiles.base_from_zoom,
            before_zoom=ctx.tiles.base_before_zoom,
        )
        generate_tiles(
            tiles_layer=TilesLayer.POI,
            x=ctx.tiles.x,
            y=ctx.tiles.y,
            z=ctx.tiles.z,
            from_zoom=ctx.tiles.poi_from_zoom,
            before_zoom=ctx.tiles.poi_before_zoom,
        )
    else:
        logging.info("no parameter given for tile generation, skipping it")

@task
def init_osm_update(ctx):
    """
    Init osmosis folder with configuration files and
    latest state.txt file before .pbf timestamp
    """
    logging.info("initializing osm update...")
    session = requests.Session()

    class OsmState(BaseModel):
        sequenceNumber: int
        timestamp: datetime

    def get_state_url(sequence_number=None):
        base_url = ctx.osm_update.replication_url
        if sequence_number is None:
            # Get last state.txt
            return f'{base_url}/state.txt'
        else:
            return f'{base_url}' \
                f'/{sequence_number // 1_000_000 :03d}' \
                f'/{sequence_number // 1000 % 1000 :03d}' \
                f'/{sequence_number  % 1000 :03d}.state.txt'

    def get_state(sequence_number=None):
        url = get_state_url(sequence_number)
        resp = session.get(url)
        resp.raise_for_status()
        # state file may contain escaped ':' in the timestamp
        state_string = resp.text.replace('\:',':')
        c = configparser.ConfigParser()
        c.read_string('[root]\n'+state_string)
        return OsmState(**c['root'])

    # Init osmosis working directory
    ctx.run(f'mkdir -p {ctx.update_tiles_dir}')
    ctx.run(f'touch {ctx.update_tiles_dir}/download.lock')

    raw_osm_datetime = ctx.run(f'osmconvert {ctx.osm.file} --out-timestamp').stdout
    osm_datetime = parse(raw_osm_datetime)
    # Rewind 2 hours as a precaution
    osm_datetime -= timedelta(hours=2)

    last_state = get_state()
    sequence_number = last_state.sequenceNumber
    sequence_dt = last_state.timestamp

    for i in range(ctx.osm_update.max_interations):
        if sequence_dt < osm_datetime:
            break
        sequence_number -= 1
        state = get_state(sequence_number)
        sequence_dt = state.timestamp
    else:
        logging.error('Failed to init osm update. ' \
            'Could not find a replication sequence before %s', osm_datetime)
        return

    state_url = get_state_url(sequence_number)
    ctx.run(f'wget -q "{state_url}" -O {ctx.update_tiles_dir}/state.txt')
    ctx.run(f'echo "baseUrl={ctx.osm_update.replication_url}" > {ctx.update_tiles_dir}/configuration.txt')
    ctx.run(f'echo "maxInterval = {ctx.osm_update.max_interval}" >> {ctx.update_tiles_dir}/configuration.txt')

@task(default=True)
def load_all(ctx):
    """
    default task called if `invoke` is run without args

    This is the main tasks that import all the datas into postgres and start the tiles generation process
    """
    if not ctx.osm.file and not ctx.osm.url:
        raise Exception("you should provide a osm.file variable or osm.url variable")

    load_osm(ctx)
    load_additional_data(ctx)
    run_post_sql_scripts(ctx)
    generate_tiles(ctx)
    init_osm_update(ctx)
