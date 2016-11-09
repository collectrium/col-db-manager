#!/bin/sh

python -m nose \
    --verbose \
    --tests=tests \
    --no-byte-compile \
    --with-coverage \
    --cover-package=omnia \
    --cover-min-percentage=90

if [ $? -ne 0 ]; then exit 1; fi

python -m pylint omnia

