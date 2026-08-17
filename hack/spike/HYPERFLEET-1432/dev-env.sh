# Dev Postgres defaults (match Makefile db/setup). Override via env if needed.
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-hyperfleet}"
export PGPASSWORD="${PGPASSWORD:-foobar-bizz-buzz}"
export PGDATABASE="${PGDATABASE:-hyperfleet}"
export PAGER=cat  # avoid psql pager blocking script exit
