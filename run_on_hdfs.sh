#!/usr/bin/env bash

set -e
set -o pipefail

: "${PACKAGE:=task_1}"

: "${JAR_FILE:=$(pwd)/out/artifacts/hw_part_1_jar/hw_part_1.jar}"
: "${MAIN_CLASS:=${PACKAGE}.ProductAvgDriver}"
: "${INPUT_FILE:=/home/cloudera/Desktop/data_samples/samples_100.json}"
: "${OUTPUT_FILE:=/tmp/${PACKAGE}__${RANDOM}.txt}"

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
hadoop fs -getmerge ${WRK_DIR}/output/part-* ${OUTPUT_FILE}
cat ${OUTPUT_FILE}
