# tap-eloqua
Singer tap to extract data from the Eloqua API, conforming to the Singer
spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

Eloqua API: https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/

## Setup

`python3 setup.py install`

## Running the tap

#### Discover mode:

`tap-eloqua --config config.json --discover > catalog.json`

#### Sync mode:

`tap-eloqua --config config.json -p catalog.json -s state.json`