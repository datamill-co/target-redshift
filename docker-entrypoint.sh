#!/usr/bin/env bash

python -m venv venv--target-redshift
source /code/venv--target-redshift/bin/activate

pip install -e .[tests]

echo -e "\n\nINFO: Dev environment ready."

tail -f /dev/null
