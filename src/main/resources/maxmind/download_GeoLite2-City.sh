#!/usr/bin/env bash
TARGET_FILE=./src/test/resources/maxmind/GeoLite2-City.mmdb

usage() { echo "Usage: $0 [-f] (force download)" 1>&2; exit 1; }

while getopts ":f" o; do
    case "${o}" in
        f)
            echo rm -rf $TARGET_FILE
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -f $TARGET_FILE ];
then
  echo $TARGET_FILE already exists
else
  echo Downloading GeoLite2-City.mmdb
  aws s3 cp s3://seekingalpha-data/accessories/maxmind/FREEZE/GeoLite2-City.mmdb $TARGET_FILE
  echo Note: some of the tests might fail if the contents of the DB has changed
fi