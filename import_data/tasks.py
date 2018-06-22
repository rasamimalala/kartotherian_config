import logging
import invoke
from invoke import task
import os.path


@task
def load_basemap(ctx):
    ctx.run(f'time imposm3 \
  import \
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.database}" \
  -read {ctx.osm_file} \
  -diff \
  -mapping {ctx.main_dir}/generated_mapping_base.yaml \
  -deployproduction -overwritecache \
  -optimize \
  -diffdir {ctx.generated_files_dir}/diff/base -cachedir {ctx.generated_files_dir}/cache/base')


@task
def load_poi(ctx):
    ctx.run(f'time imposm3 \
  import \
  -write --connection "postgis://{ctx.pg.user}:{ctx.pg.password}@{ctx.pg.host}/{ctx.pg.database}" \
  -read {ctx.osm_file} \
  -diff \
  -mapping {ctx.main_dir}/generated_mapping_poi.yaml \
  -deployproduction -overwritecache \
  -optimize \
  -diffdir {ctx.generated_files_dir}/diff/poi -cachedir {ctx.generated_files_dir}/cache/poi')


def _run_sql_script(ctx, script_name):
    ctx.run(f"psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.database} --set ON_ERROR_STOP='1' -f {ctx.sql_dir}/{script_name}",
           env={'PGPASSWORD': ctx.pg.password})


@task
def run_sql_script(ctx):
    # load several psql functions
    _run_sql_script(ctx, "language.sql")
    _run_sql_script(ctx, "postgis-vt-util.sql")
    # localization sql function
    _run_sql_script(ctx, "get_localized_name.sql")
    _run_sql_script(ctx, "get_localized_name_from_tags.sql")
    _run_sql_script(ctx, "get_country.sql")
    _run_sql_script(ctx, "get_country_name.sql")
    _run_sql_script(ctx, "geo_transliterate.sql")


@task
def import_natural_earth(ctx):
    print('importing natural earth shapes in postgres')
    target_file = f"{ctx.data_dir}/natural_earth_vector.sqlite"

    if not os.path.isfile(target_file):
        ctx.run(f"wget --progress=dot:giga http://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip \
        && unzip -oj natural_earth_vector.sqlite.zip -d {ctx.data_dir} \
        && rm natural_earth_vector.sqlite.zip")

    pg_conn = f'dbname={ctx.pg.database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}'
    ctx.run(f'PGCLIENTENCODING=LATIN1 ogr2ogr \
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
    {ctx.data_dir}/natural_earth_vector.sqlite')


@task
def import_water_polygon(ctx):
    print('importing water polygon shapes in postgres')

    target_file = f"{ctx.data_dir}/water_polygons.shp"
    if not os.path.isfile(target_file):
        ctx.run(f"wget --progress=dot:giga http://data.openstreetmapdata.com/water-polygons-split-3857.zip \
    && unzip -oj water-polygons-split-3857.zip -d {ctx.data_dir} \
    && rm water-polygons-split-3857.zip")

    ctx.run(f"POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DATA_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import-water.sh")


@task
def import_lake(ctx):
    print('importing the lakes borders in postgres')

    target_file = f"{ctx.data_dir}/lake_centerline.geojson"
    if not os.path.isfile(target_file):
        ctx.run(f"wget --progress=dot:giga -L -P {ctx.data_dir} https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.geojson")

    pg_conn = f'dbname={ctx.pg.database} user={ctx.pg.user} password={ctx.pg.password} host={ctx.pg.host}'
    ctx.run(f'PGCLIENTENCODING=UTF8 ogr2ogr \
    -f Postgresql \
    -s_srs EPSG:4326 \
    -t_srs EPSG:3857 \
    PG:"{pg_conn}" \
    {ctx.data_dir}/lake_centerline.geojson \
    -overwrite \
    -nln "lake_centerline"')


@task
def import_border(ctx):
    print('importing the borders in postgres')

    target_file = f"{ctx.data_dir}/osmborder_lines.csv"
    if not os.path.isfile(target_file):
        ctx.run(f"wget --progress=dot:giga -P {ctx.data_dir} https://github.com/openmaptiles/import-osmborder/releases/download/v0.4/osmborder_lines.csv.gz \
    && gzip -d {ctx.data_dir}/osmborder_lines.csv.gz")

    ctx.run(f'POSTGRES_PASSWORD={ctx.pg.password} POSTGRES_PORT={ctx.pg.port} IMPORT_DIR={ctx.data_dir} \
  POSTGRES_HOST={ctx.pg.host} POSTGRES_DB={ctx.pg.database} POSTGRES_USER={ctx.pg.user} \
  {ctx.main_dir}/import_osmborder_lines.sh')


@task
def import_wikidata(ctx):
    """
    import wikidata (for some translations)

    For the moment this does nothing (but we need a table for some openmaptiles function)
    """
    create_table = "CREATE TABLE IF NOT EXISTS wd_names (id          varchar(20) UNIQUE, page          varchar(200) UNIQUE,    labels      hstore);"
    ctx.run(f"psql -Xq -h {ctx.pg.host} -U {ctx.pg.user} -d {ctx.pg.database} {create_table}",
            env={'PGPASSWORD': ctx.pg.password})


@task
def run_post_sql_scripts(ctx):
    """
    load the sql file with all the functions to generate the layers
    this file has been generated using https://github.com/QwantResearch/openmaptiles
    """
    print('running postsql scripts')
    _run_sql_script(ctx, "generated_base.sql")
    _run_sql_script(ctx, "generated_poi.sql")


@task
def load_osm(ctx):
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


@task(default=True)
def load_all(ctx):
    """
    default task called if `invoke` is run without args

    This is the main tasks that import all the datas into postgres
    """
    if ctx.osm_file is None:
        raise Exception("you should provide a osm_file variable")

    load_osm(ctx)
    load_additional_data(ctx)
    run_post_sql_scripts(ctx)
