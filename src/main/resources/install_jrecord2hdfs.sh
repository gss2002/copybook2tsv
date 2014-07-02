#!/bin/bash
binDir=`dirname "$0"`
echo $binDir
hadoop fs -mkdir -p /apps/copybook2tsv
hadoop fs -copyFromLocal -f $binDir/JRecordV2.jar /apps/copybook2tsv/