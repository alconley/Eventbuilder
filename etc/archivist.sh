#!/bin/bash

RUNNO=$1
BINARYDIR=/home/alconley/Projects/January_2025_207Bi/DAQ/run_$RUNNO/UNFILTERED

ARCHIVE=/home/alconley/Projects/January_2025_207Bi/WorkingDir/raw_binary/run_$RUNNO.tar.gz

echo "Running archivist for binary data in $BINARYDIR to archive $ARCHIVE..."

cd $BINARYDIR

tar -cvzf $ARCHIVE ./*.BIN

cd -

echo "Complete."