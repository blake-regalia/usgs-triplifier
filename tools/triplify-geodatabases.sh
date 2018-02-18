#!/bin/bash

# triplify geodatabases
pushd ../
	node --max-old-space-size=8192 ./lib/geoms/gdb-psql.js ./data/input/geodatabases/*.zip
	node --max-old-space-size=8192 ./lib/geoms/psql-ttl.js ./data/input/geodatabases/*.sql
popd
