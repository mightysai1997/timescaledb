#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests "$@" -v8 \
          2.8.1-pg15