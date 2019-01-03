# Configuration files for kartotherian stack

## Cloning this repository

Make sure to clone the submodules as well

```
git clone --recurse-submodules https://github.com/QwantResearch/kartotherian_config
```

## import_data

This folder contains all the scripts and details to import OSM (and Natural
Earth) data into a PostgreSQL database.

## Imposm

 * `generated_mapping_{base,poi}.yml` : Imposm mapping (based on openmaptiles) for basemap and POIs
 * `generated_{base,poi}.sql` : SQL views, functions and triggers used by vector style

## Tilerator

 * `sources.yaml` : sources used for tile generation (with substantial for empty tiles filtering, etc.)
 * `data_tm2source.xml` and `data_tm2source_lite.xml` : vector style 

## Kartotherian
 
 * `sources.yaml` : sources used by tileserv (no tile generation)
