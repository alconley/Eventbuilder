#!/bin/bash

RUNNO=$1
BINARYDIR=/Users/alconley/Projects/CatrinaTests/DAQ/run_$RUNNO/UNFILTERED

ARCHIVE=/Users/alconley/Projects/CatrinaTests/WorkingDir/raw_binary/run_$RUNNO.tar.gz

echo "Running archivist for binary data in $BINARYDIR to archive $ARCHIVE..."

cd $BINARYDIR

tar -cvzf $ARCHIVE ./*.BIN

cd -

echo "Complete."