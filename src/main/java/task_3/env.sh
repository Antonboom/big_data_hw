#!/usr/bin/env bash

: "${MAIN_CLASS:=task_3.ProductFilteredDriver}"
: "${INPUT_FILES:=${DATA_PATH}/prodname_avg_rating.csv}"
: "${OUTPUT_FILE:=${DATA_PATH}/prodname_avg_rating_filtered.csv}"
: "${JAR_ADDITIONAL_ARGS:=Nook}"
