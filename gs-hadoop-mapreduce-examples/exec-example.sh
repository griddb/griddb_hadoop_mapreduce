#! /bin/bash

function parse_args() {

    num_containers=2
    options=()
    files=()

    while [ "$1" != "" ]
    do
        case $1 in
            --job)                      shift; job=$1 ;;
            --num-containers)           shift; num_containers=$1 ;;
            --file)                     shift; options=("${options[@]}" "--file $1") ;;
            --define)                   shift; options=("${options[@]}" "--define $1") ;;
            *)                          files=("${files[@]}" $1) ;;
        esac
        shift
    done

    jobname="${job,,}"

    usage_common="      [--num-containers <num_containers>]\n   [--file <property_filename>]\n  [--define <key=value>] ...\n    <filename> ..."
    usage_wordcount="Usage: $0 --job wordcount\n$usage_common"

    usage="$usage_wordcount"

    if [ "${jobname-}" == "" ]; then
        echo -e "$usage"
        exit 1
    elif [ "$jobname" == "wordcount" ]; then
        if [[ ${#options[@]} == 0 || ${#files[@]} == 0 ]]; then
            echo -e "$usage_wordcount"
            exit 1
        fi
    else
        echo "Unknown job: $job"
        exit 1
    fi
}

GS_HADOOP_MAPREDUCE_HOME=${GS_HADOOP_MAPREDUCE_HOME:-..}
GS_HADOOP_MAPREDUCE_VERSION=${GS_HADOOP_MAPREDUCE_VERSION:-1.0.0}

GRIDSTORE_JAR=${GRIDSTORE_JAR:-${GS_HADOOP_MAPREDUCE_HOME}/lib/gridstore.jar}
GS_HADOOP_MAPREDUCE_CLIENT_JAR=${GS_HADOOP_MAPREDUCE_CLIENT_JAR:-${GS_HADOOP_MAPREDUCE_HOME}/gs-hadoop-mapreduce-client/target/gs-hadoop-mapreduce-client-${GS_HADOOP_MAPREDUCE_VERSION}.jar}
GS_HADOOP_MAPREDUCE_EXAMPLES_JAR=${GS_HADOOP_MAPREDUCE_EXAMPLES_JAR:-${GS_HADOOP_MAPREDUCE_HOME}/gs-hadoop-mapreduce-examples/target/gs-hadoop-mapreduce-examples-${GS_HADOOP_MAPREDUCE_VERSION}.jar}

GS_INPUT_CONTAINER_PREFIX=${GS_INPUT_CONTAINER_PREFIX:-input}
GS_OUTPUT_CONTAINER_NAME=${GS_OUTPUT_CONTAINER_NAME:-output}

classpath=${GS_HADOOP_MAPREDUCE_EXAMPLES_JAR}:${GS_HADOOP_MAPREDUCE_CLIENT_JAR}:${GRIDSTORE_JAR}

hadoop_classpath=${GS_HADOOP_MAPREDUCE_CLIENT_JAR}:${GRIDSTORE_JAR}
[ -n "${HADOOP_CLASSPATH-}" ] && hadoop_classpath=${hadoop_classpath}:${HADOOP_CLASSPATH}
export HADOOP_CLASSPATH=$hadoop_classpath

libjars=${GS_HADOOP_MAPREDUCE_CLIENT_JAR},${GRIDSTORE_JAR}

parse_args $*

java -cp $classpath com.toshiba.mwcloud.gs.hadoop.mapreduce.examples.tool.GSTool prepare --job $jobname --input $GS_INPUT_CONTAINER_PREFIX --num-containers $num_containers --output $GS_OUTPUT_CONTAINER_NAME ${options[@]} ${files[@]}

if [ $? != 0 ]; then
    exit $?
fi

if [ "$jobname" == "wordcount" ]; then
    hadoop jar $GS_HADOOP_MAPREDUCE_EXAMPLES_JAR com.toshiba.mwcloud.gs.hadoop.mapreduce.examples.GSWordCount -libjars $libjars -D mapreduce.job.maps=$num_containers ${options[@]} ${GS_INPUT_CONTAINER_PREFIX}'_[0-9]*' $GS_OUTPUT_CONTAINER_NAME
fi

if [ $? != 0 ]; then
    exit $?
fi

java -cp $classpath com.toshiba.mwcloud.gs.hadoop.mapreduce.examples.tool.GSTool delete --input $GS_INPUT_CONTAINER_PREFIX --num-containers $num_containers --output $GS_OUTPUT_CONTAINER_NAME ${options[@]}

exit $?
