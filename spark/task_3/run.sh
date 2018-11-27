#!/usr/bin/env bash

set -e

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON_OPTS=""

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_3/output

spark2-submit --master yarn product_filtered.py \
    hdfs:///user/cloudera/hw_part_1/spark/task_3/data/prodname_avg_rating.csv \
    hdfs:///user/cloudera/hw_part_1/spark/task_3/output \
    Nook

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_3/output

hadoop fs -getmerge \
    /user/cloudera/hw_part_1/spark/task_3/output/part-* \
    /home/cloudera/Desktop/data_samples/prodname_avg_rating_filtered.csv

head /home/cloudera/Desktop/data_samples/prodname_avg_rating_filtered.csv -n 10

