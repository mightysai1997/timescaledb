#!/usr/bin/env bash

set -u
set -e

PG_REGRESS_PSQL=$1
PSQL=${PSQL:-$PG_REGRESS_PSQL}
TEST_PGUSER=${TEST_PGUSER:-postgres}
TEST_TABLESPACE1_PATH=${TEST_TABLESPACE1_PATH:-$(mktemp -d)}
TEST_TABLESPACE2_PATH=${TEST_TABLESPACE2_PATH:-$(mktemp -d)}

shift

function cleanup {
    rm -rf ${TEST_TABLESPACE1_PATH}
    rm -rf ${TEST_TABLESPACE2_PATH}
}

trap cleanup EXIT

# Setup directories required by tests
cd test/sql
mkdir -p ${TEST_TABLESPACE1_PATH}
mkdir -p ${TEST_TABLESPACE2_PATH}
mkdir -p dump

# Hack to grant TEST_PGUSER superuser status so that we can
# consistently run tests using the same user rather than the
# current/local user
${PSQL} $@ -v ECHO=none -c "ALTER USER ${TEST_PGUSER} WITH SUPERUSER;"

exec ${PSQL} -U ${TEST_PGUSER} -v ON_ERROR_STOP=1 -v VERBOSITY=terse -v ECHO=all -v TEST_TABLESPACE1_PATH=\'${TEST_TABLESPACE1_PATH}\' -v TEST_TABLESPACE2_PATH=\'${TEST_TABLESPACE2_PATH}\' $@
