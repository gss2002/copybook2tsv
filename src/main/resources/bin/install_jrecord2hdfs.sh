#!/bin/bash
binDir=`dirname "$0"`
echo $binDir

HADOOP_HOME_PATH=/usr/hdp/current/hadoop-client
HADOOP_CONFIG_SCRIPT=$HADOOP_HOME_PATH/libexec/hadoop-config.sh
HADOOP_CLIENT_LIBS=$HADOOP_HOME_PATH/client
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
else
        echo "Hadoop Client not Installed on Node"
        exit 1
fi

JRECORDJAR=`ls -1 $binDir/lib/JRecord*.jar`
export HADOOP_CLASSPATH=$JRECORDJAR
COPYBOOKJAR=`ls -1 $binDir/lib/copybook2tsv*.jar`

hadoop fs -mkdir -p /apps/copybook2tsv
hadoop fs -copyFromLocal -f $JRECORDJAR /apps/copybook2tsv/JRecordV2.jar