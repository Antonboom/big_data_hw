#!/usr/bin/env bash

set -e -u -o pipefail

: "${PACKAGE:=task_1}"  # task_1, task_2, task_3

: "${DATA_PATH:=/home/cloudera/Desktop/data_samples}"
: "${JAR_FILE:=$(pwd)/out/artifacts/hw_part_1_jar/hw_part_1.jar}"

source "$(pwd)/src/main/java/${PACKAGE}/env.sh"

: "${MAIN_CLASS}"
: "${INPUT_FILE}"
: "${OUTPUT_FILE:=/tmp/${PACKAGE}__${RANDOM}.csv}"

USER_DIR=/user/cloudera
WRK_DIR=${USER_DIR}/hw_part_1/${PACKAGE}

printf "Prepare HDFS working directory...\n"
hdfs dfs -rm -r -f ${WRK_DIR}/*
hdfs dfs -rm -r -f ${USER_DIR}/.Trash
hdfs dfs -mkdir -p ${WRK_DIR}/data

printf "\nCopy ${INPUT_FILE} to HDFS workdir...\n"
JAR_INPUT="hdfs:///${WRK_DIR}/data/$(basename ${INPUT_FILE})"
hdfs dfs -copyFromLocal ${INPUT_FILE} ${JAR_INPUT}

printf "\nRun ${JAR_FILE}...\n"
hadoop jar ${JAR_FILE} ${MAIN_CLASS} ${JAR_INPUT} ${WRK_DIR}/output

printf "\nHDFS workdir:\n"
hdfs dfs -ls -R ${WRK_DIR}

printf "\nMerge files...\nResult in ${OUTPUT_FILE}\n"
rm -f ${OUTPUT_FILE}
hadoop fs -getmerge ${WRK_DIR}/output/part-* ${OUTPUT_FILE}
head ${OUTPUT_FILE} -n 10
