#!/bin/bash

exit

#
source eups-setups.sh

#
if [ -d  tests/.tests ] && \
    [ "`ls tests/.tests/*\.failed 2> /dev/null | wc -l`" -ne 0 ]; then
    echo "*** Failed unit tests."; 
    exit 1 
fi
