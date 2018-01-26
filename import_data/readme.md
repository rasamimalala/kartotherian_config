Python script to import all the data into postgres

The script is based on [invoke](https://github.com/pyinvoke/)

## install
You need [pipenv](https://github.com/pypa/pipenv)

`pipenv install`

## run
To import all data:

`INVOKE_OSM_FILE=path_to_an_osm_file pipenv run invoke `

Note:
the osm file can also be put in a `invoke.yaml` file 

To only run one task:

`INVOKE_OSM_FILE=path_to_an_osm_file pipenv run invoke <one_task>`
eg.

`INVOKE_OSM_FILE=path_to_an_osm_file pipenv run invoke load-poi`

Note: be careful to replace `_` with `-` in the function name
