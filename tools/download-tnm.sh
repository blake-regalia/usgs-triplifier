#!/bin/bash

cd "${BASH_SOURCE%/*}" || exit
pushd ../
	node ./lib/tnm/download.js
popd
