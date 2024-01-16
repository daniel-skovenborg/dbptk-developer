#!/usr/bin/env bash

#ARGS:
# source database name
# destination deatabase name
# temporary user username

#ENVIRONMENT VARS:
# DPT_POSTGRESQL_USER - mysql username for a user with permission to create a new user (defaults to "postgres")
# DPT_POSTGRESQL_PASS - mysql password for the DPT_POSTGRESQL_USER user (defaults to empty)

TEST_DB_SOURCE="$1"
TEST_DB_TARGET="$2"
TEST_DB_USER="$3"

if [ -z "$DPT_POSTGRESQL_USER" ]
then
  export PGUSER="postgres"
else
  export PGUSER="$DPT_POSTGRESQL_USER"
fi
export PGPASSWORD="$DPT_POSTGRESQL_PASS"

function sql() {
  psql -q -h 127.0.0.1 -d postgres --command="$1"
}

sql "DROP DATABASE IF EXISTS \"$TEST_DB_SOURCE\";"
sql "DROP DATABASE IF EXISTS \"$TEST_DB_TARGET\";"
sql "REVOKE ALL PRIVILEGES ON SCHEMA public FROM \"$TEST_DB_USER\";"
sql "DROP ROLE IF EXISTS \"$TEST_DB_USER\";"
