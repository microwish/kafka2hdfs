#!/usr/bin/env bash

# crontab
# */1 * * * * /data/users/data-infra/kafka2hdfs/kafka2hdfs.sh 5 >> /data/users/data-infra/kafka2hdfs/kafka2hdfs.sh.log 2>&1

# libjvm.so

concat_jar_paths()
{
    str=$1
    arr=(${str//:/ })
    paths=
    for s in ${arr[*]}
    do
        paths="$paths:$s"
    done
    echo $paths
}

classpaths=$(hadoop classpath)
classpaths=$(concat_jar_paths $classpaths)

ulimit -c unlimited
ulimit -d unlimited
ulimit -f unlimited
ulimit -m unlimited
ulimit -s 20480
ulimit -v unlimited

export LD_LIBRARY_PATH="/data/users/data-infra/kafkaclient/cpp:/usr/lib/hadoop/lib/native:/usr/jdk64/jdk1.7.0_45/jre/lib/amd64/server:/usr/local/lib:$LD_LIBRARY_PATH"

export CLASSPATH=$classpaths

export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/lib/hadoop/lib/native -Djava.library.path=/usr/lib/hadoop/lib"

export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/lib/hadoop/lib/native

num=$1

procs=$(ps -ef | grep 'kafka2hdfs' | grep -v 'grep' | grep -vi 'java')

for ((i=0; i<$num; i++))
do
    if echo "$procs" | grep -q "kafka2hdfs$i.conf"
    then
        continue
    else
        YmdHMs=$(date +%Y%m%d%H%M%S)
        echo "[$YmdHMs]kafka2hdfs#$i crashed or aborted"
        /data/users/data-infra/kafka2hdfs/kafka2hdfs -k /data/users/data-infra/kafka2hdfs/kafka2hdfs$i.conf -c /data/users/data-infra/kafka2hdfs/consumer$i.properties -l /data/users/data-infra/kafka2hdfs/kafka2hdfs$i.log > /data/users/data-infra/kafka2hdfs/kafka2hdfs_stderr$i.log 2>&1 &
        #valgrind -v --leak-check=full --show-reachable=yes --track-origins=yes --trace-children=yes --log-file=/data/users/data-infra/ka    fka2hdfs/memcheck$i.valgrind /data/users/data-infra/kafka2hdfs/kafka2hdfs -k /data/users/data-infra/kafka2hdfs/kafka2hdfs$i.conf -c /data/users/data-infra/kafka2hdfs/consumer$i.properties -l /data/users/data-infra/kafka2hdfs/kafka2hdfs$i.log > /data/users/data-infra/kafka2hdfs/kafka2hdfs_stderr$i.log 2>&1 &
        sleep 2
    fi
done
