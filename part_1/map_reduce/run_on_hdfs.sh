#!/usr/bin/env bash

set -e -u -o pipefail

: "${PACKAGE:=task_1}"  # task_1, task_2, task_3

: "${DATA_PATH:=/home/cloudera/Desktop/data_samples}"
: "${JAR_FILE:=$(pwd)/out/artifacts/hw_part_1_jar/hw_part_1.jar}"

source "$(pwd)/src/main/java/${PACKAGE}/env.sh"

: "${MAIN_CLASS}"
: "${INPUT_FILES}"
: "${OUTPUT_FILE:=/tmp/${PACKAGE}__${RANDOM}.csv}"
: "${JAR_ADDITIONAL_ARGS:=}"

USER_DIR=/user/cloudera
WRK_DIR=${USER_DIR}/hw_part_1/${PACKAGE}

printf "Prepare HDFS working directory...\n"
hdfs dfs -rm -r -f -skipTrash ${WRK_DIR}/*
hdfs dfs -mkdir -p ${WRK_DIR}/data

JAR_INPUT=""
for input_file in ${INPUT_FILES[@]}; do
    printf "\nCopy ${input_file} to HDFS workdir...\n"
    FILE_PATH_ON_HDFS="hdfs:///${WRK_DIR}/data/$(basename ${input_file})"
    JAR_INPUT="${JAR_INPUT} ${FILE_PATH_ON_HDFS}"
    hdfs dfs -copyFromLocal ${input_file} ${FILE_PATH_ON_HDFS}
done

printf "\nRun ${MAIN_CLASS} from ${JAR_FILE}...\n"
hadoop jar ${JAR_FILE} ${MAIN_CLASS} ${JAR_INPUT} ${WRK_DIR}/output ${JAR_ADDITIONAL_ARGS}

printf "\nHDFS workdir:\n"
hdfs dfs -ls -R ${WRK_DIR}

printf "\nMerge files...\nResult in ${OUTPUT_FILE}\n"
rm -f ${OUTPUT_FILE}
hadoop fs -getmerge ${WRK_DIR}/output/part-* ${OUTPUT_FILE}
head ${OUTPUT_FILE} -n 10
