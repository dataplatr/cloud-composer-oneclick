#!/bin/bash

# Interactive Configurator Launcher

# Exit on error.
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${SCRIPT_DIR}" 1> /dev/null
echo -n "Please wait while we are checking dependencies..."
python3 -m pip install --use-pep517 --disable-pip-version-check --require-hashes --no-deps -r "requirements.txt" 1>/dev/null && \
python3 -m pip install --use-pep517 --disable-pip-version-check --require-hashes --no-deps -r "../../../requirements.txt" 1>/dev/null
echo -e -n "\r                                                    \r"
popd 1> /dev/null

export PYTHONPATH=$$PYTHONPATH:${SCRIPT_DIR}:.
pushd "${SCRIPT_DIR}/../../.." 1> /dev/null # Data Foundation root
python3 "${SCRIPT_DIR}/main.py" "${1}" "${2}" "${3}"
source_project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectId']))" 2>/dev/null || echo "")
gcloud config set project "${source_project}"
chmod +x deploy.sh
./deploy.sh
popd 1> /dev/null