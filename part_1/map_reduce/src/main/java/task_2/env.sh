#!/usr/bin/env bash

: "${MAIN_CLASS:=task_2.ProductNameRatingDriver}"
: "${INPUT_FILES:=${DATA_PATH}/avg_rating.csv ${DATA_PATH}/Electronics_meta.json}"  # samples_100_meta.json, Electronics_meta.json
: "${OUTPUT_FILE:=${DATA_PATH}/prodname_avg_rating.csv}"
