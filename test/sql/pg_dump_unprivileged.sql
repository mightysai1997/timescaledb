
\c template1 :ROLE_SUPERUSER

SET client_min_messages TO ERROR;
CREATE EXTENSION IF NOT EXISTS timescaledb;
RESET client_min_messages;

CREATE USER dump_unprivileged CREATEDB;

\c template1 dump_unprivileged
CREATE database dump_unprivileged;

\! utils/pg_dump_unprivileged.sh

\c template1 :ROLE_SUPERUSER
DROP EXTENSION timescaledb;
DROP DATABASE dump_unprivileged;
DROP USER dump_unprivileged;

