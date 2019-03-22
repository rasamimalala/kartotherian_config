## How to run this project

Requirements: Python 3.6+, pip

```bash

git clone -b test-generate-tiles https://github.com/QwantResearch/kartotherian_config.git
cd kartotherian_config/import_data

# Install pipenv
pip3 install --user pipenv

# Create virtualenv and install all dependencies from Pipfile.lock
pipenv install --dev --deploy 

# Run tests (1 test will fail for now)
pipenv run pytest

```

## Goal: Implement tiles generation with multiple base tiles

### Current implementation 

In the Kartotherian stack, [Tilerator](https://github.com/kartotherian/tilerator) is an API to manage tiles generation jobs.
To make this operation easier, a task "generate_tiles" is implemented using [Invoke tasks](https://www.pyinvoke.org/).

There are currently two ways to create tiles generation jobs:

* Invoke the `generate-tiles` task with a "planet" flag enabled:

```bash
INVOKE_TILES_PLANET=1 pipenv run invoke generate-tiles 
``` 

* Invoke the `generate-tiles` task with a tile number to use as the tip of a pyramid of tiles

To generate all tiles in Luxembourg (below tile z=7, x=66, y=43)

```bash
INVOKE_TILES_X=66 -e INVOKE_TILES_Y=43 -e INVOKE_TILES_Z=7 pipenv run invoke generate-tiles 
```

### What needs to be changed

We would like to implement a new `bases` parameter to start the generation
process over multiple base tiles at once, using the `{z}/{x}/{y}` format.

For example, to generate all tiles in Metropolitan France, we would run:

```bash
INVOKE_TILES_BASES=5/15/10,5/16/10,5/15/11,5/16/11 pipenv run invoke generate-tiles
```

Complete the [`generate_tiles`](https://github.com/QwantResearch/kartotherian_config/blob/test-generate-tiles/import_data/tasks.py#L423) task in tasks.py to implement this behavior.

A test has already been implemented in "test_tasks.py"  
You can test your implementation with `pipenv run pytest`

