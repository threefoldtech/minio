#!/bin/sh
set -e

# This file is used by CI to build an archive with
# all the binaries, and config files for flist building

archive=$1

if [ -z "${archive}" ]; then
    echo "missing argument" >&2
    exit 1
fi

# Absolute path to this script. /home/user/bin/foo.sh
script=$(readlink -f $0)
# Absolute path this script is in. /home/user/bin
scriptpath=`dirname $script`

mkdir -p ${archive}/bin ${archive}/etc ${archive}/data
cp -r ${scriptpath}/rootfs/* ${archive}/
cp ./minio ${archive}/bin/
cp ./entrypoint ${archive}/bin/minioentry
