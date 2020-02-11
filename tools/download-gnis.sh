#!/bin/bash

# download data files
gnis_version=${1:-$USGS_GNIS_VERSION_CODE}
function download_gnis_file {
	file="$2.zip"
	curl --create-dirs \
		-H "Accept-Encoding: gzip, deflate" \
		-H "Connection: keep alive" --keepalive-time 2 \
		-o "./data/input/gnis/$1.zip" "https://geonames.usgs.gov/docs/stategaz/$file"
}

cd "${BASH_SOURCE%/*}" || exit
pushd ../
	download_gnis_file units "GOVT_UNITS"
	download_gnis_file features "NationalFile"
	download_gnis_file names "AllNames"
	download_gnis_file history "Feature_Description_History"
popd
