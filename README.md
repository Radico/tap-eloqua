# tap-eloqua
Singer tap to extract data from the Eloqua API, conforming to the Singer
spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

Eloqua API: https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/

## Setup

### Development

1. Setup virtual environment: `virtualenv --python=3.7 .venv`
1. Activate: `source .venv/bin/activate`
1. Install packages: `poetry install`
    1. Install poetry with: `curl -sSL https://install.python-poetry.org | python3 -`
1. Enable the virtualenv where poetry installed: `source $(poetry env info --path)/bin/activate`

## Running the tap

#### Discover mode:

`tap-eloqua --config config.json --discover > catalog.json`

#### Sync mode:

`tap-eloqua --config config.json -p catalog.json -s state.json`
