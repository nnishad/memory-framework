#!/bin/sh
# Run from the extracted release directory on the Hermes host.
set -eu
umask 077
memory_venv="${MEMORY_VENV:-$HOME/.local/share/hermes-memory/venv}"
python3 -m venv "$memory_venv"
"$memory_venv/bin/python" -m pip install --upgrade 'pip>=26.2'
"$memory_venv/bin/python" -m pip install -c deployment/constraints-tested.txt '.[production]'
"$memory_venv/bin/python" -m personal_memory setup --hermes-home "${HERMES_HOME:-$HOME/.hermes}" --exclusive
printf '%s\n' 'Installed with managed Hindsight enabled by default. Start the launcher and use the service template in deployment/.'
