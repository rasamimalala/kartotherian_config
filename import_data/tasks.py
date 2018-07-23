import logging
import invoke
from invoke import task
import os.path
import requests
from enum import Enum

logging.basicConfig(level=logging.INFO)


def _execute_sql(ctx, sql, db=None, additional_options=''):
    query = f'psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -c "{sql}" {additional_options}'
    if db is not None:
        query += f" -d {db}"
    return ctx.run(
        query,
        env={"PGPASSWORD": ctx.pg.password},
    )


@task
def prepare_db(ctx):
    """
    creates the import database and remove the old backup one
    """
    _execute_sql(ctx, f"DROP DATABASE IF EXISTS {ctx.pg.backup_database};")
    has_db = _execute_sql(ctx, f"SELECT 1 FROM pg_database WHERE datname='{ctx.pg.import_database}';", additional_options='-tA')
    logging.info(f"log ========== '{has_db.stdout}'")
    if has_db.stdout != "1\n":
        _execute_sql(ctx, f"CREATE DATABASE {ctx.pg.import_database};")
        _execute_sql(ctx, db=ctx.pg.import_database, 
        sql=f"""
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
CREATE EXTENSION unaccent;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION osml10n;""")

@task
def get_osm_data(ctx):
    """
    download the osm file and store it in the input_data directory
    """
    logging.info("downloading osm file {}")
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
def load_basemap(ctx):
    ctx.run(
        f'time imposm3 \
  import \
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.import_database}" \
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
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.import_database}" \
  -read {ctx.osm.file} \
  -diff \
  -mapping {ctx.main_dir}/generated_mapping_poi.yaml \
  -deployproduction -overwritecache \
  -optimize \
  -diffdir {ctx.generated_files_dir}/diff/poi -cachedir {ctx.generated_files_dir}/cache/poi'
    )


def _run_sql_script(ctx, script_name):
    ctx.run(
        f"psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.import_database} --set ON_ERROR_STOP='1' -f {ctx.sql_dir}/{script_name}",
        env={"PGPASSWORD": ctx.pg.password},
    )


@task
def run_sql_script(ctx):
    # load several psql functions
    _run_sql_script(ctx, "language.sql")
    _run_sql_script(ctx, "postgis-vt-util.sql")


@task
def import_natural_earth(ctx):
    print("importing natural earth shapes in postgres")
    target_file = f"{ctx.data_dir}/natural_earth_vector.sqlite"

    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga http://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip \
        && unzip -oj natural_earth_vector.sqlite.zip -d {ctx.data_dir} \
        && rm natural_earth_vector.sqlite.zip"
        )

    pg_conn = f"dbname={ctx.pg.import_database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}"
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
    print("importing water polygon shapes in postgres")

    target_file = f"{ctx.data_dir}/water_polygons.shp"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga http://data.openstreetmapdata.com/water-polygons-split-3857.zip \
    && unzip -oj water-polygons-split-3857.zip -d {ctx.data_dir} \
    && rm water-polygons-split-3857.zip"
        )

    ctx.run(
        f"POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DATA_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.import_database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import-water.sh"
    )


@task
def import_lake(ctx):
    print("importing the lakes borders in postgres")

    target_file = f"{ctx.data_dir}/lake_centerline.geojson"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga -L -P {ctx.data_dir} https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.geojson"
        )

    pg_conn = f"dbname={ctx.pg.import_database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}"
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
    print("importing the borders in postgres")

    target_file = f"{ctx.data_dir}/osmborder_lines.csv"
    if not os.path.isfile(target_file):
        ctx.run(
            f"wget --progress=dot:giga -P {ctx.data_dir} https://github.com/openmaptiles/import-osmborder/releases/download/v0.4/osmborder_lines.csv.gz \
    && gzip -d {ctx.data_dir}/osmborder_lines.csv.gz"
        )

    ctx.run(
        f"POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.import_database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import_osmborder_lines.sh"
    )


@task
def import_wikidata(ctx):
    """
    import wikidata (for some translations)

    For the moment this does nothing (but we need a table for some openmaptiles function)
    """
    create_table = "CREATE TABLE IF NOT EXISTS wd_names (id varchar(20) UNIQUE, page varchar(200) UNIQUE, labels hstore);"
    _execute_sql(ctx, db=ctx.pg.import_database, sql=create_table)


@task
def run_post_sql_scripts(ctx):
    """
    load the sql file with all the functions to generate the layers
    this file has been generated using https://github.com/QwantResearch/openmaptiles
    """
    print("running postsql scripts")
    _run_sql_script(ctx, "generated_base.sql")
    _run_sql_script(ctx, "generated_poi.sql")


@task
def load_osm(ctx):
    if ctx.osm.url:
        get_osm_data(ctx)
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
def rotate_database(ctx):
    """
    rotate the postgres database
    
    we first move the production database to a backup database, 
    then move the newly created import database to be the new production database
    """
    logging.info(f'rotating database, moving {ctx.pg.database} -> {ctx.pg.backup_database}')
    _execute_sql(ctx, f"ALTER DATABASE {ctx.pg.database} RENAME TO {ctx.pg.backup_database};", db=ctx.pg.import_database)
    logging.info(f'rotating database, moving {ctx.pg.import_database} -> {ctx.pg.database}')
    _execute_sql(ctx, f"ALTER DATABASE {ctx.pg.import_database} RENAME TO {ctx.pg.database};", db=ctx.pg.backup_database)


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


@task(default=True)
def load_all(ctx):
    """
    default task called if `invoke` is run without args

    This is the main tasks that import all the datas into postgres and start the tiles generation process
    """
    if not ctx.osm.file and not ctx.osm.url:
        raise Exception("you should provide a osm.file variable or osm.url variable")

    prepare_db(ctx)
    load_osm(ctx)
    load_additional_data(ctx)
    run_post_sql_scripts(ctx)
    rotate_database(ctx)
    generate_tiles(ctx)
