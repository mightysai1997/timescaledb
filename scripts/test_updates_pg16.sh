#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests "$@" -v8 \
          2.13.0-pg16 2.13.1-pg16

# Run repair tests for >=2.10.x versions due to PR #5441
run_tests "$@" -r -v8 \
          2.13.0-pg16 2.13.1-pg16

