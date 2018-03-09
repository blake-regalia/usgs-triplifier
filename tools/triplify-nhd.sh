#!/bin/bash

cd "${BASH_SOURCE%/*}" || exit

# triplify nhd
pushd ../
	node --max_old_space_size=8192 \
		./lib/gdb/extract ./lib/nhd/convert.js ./data/input/nhd/*.zip
popd
