#!/usr/bin/env bash

mc --vesrsion || wget https://dl.minio.io/client/mc/release/linux-amd64/mc -O /bin/mc && chmod +x /bin/mc
nosetests-3.4 -vs --logging-level=WARNING testcases.py