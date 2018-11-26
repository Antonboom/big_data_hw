#!/usr/bin/env bash

set -e

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON_OPTS=""

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_1/output

#spark2-submit --master yarn product_avg_rating.py \
#    hdfs:///user/cloudera/hw_part_1/spark/task_1/data/samples_100.json \
#    hdfs:///user/cloudera/hw_part_1/spark/task_1/output

spark2-submit --master yarn product_avg_rating.py \
    hdfs:///user/cloudera/hw_part_1/spark/task_1/data/Electronics.json \
    hdfs:///user/cloudera/hw_part_1/spark/task_1/output

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_1/output

hadoop fs -getmerge \
    /user/cloudera/hw_part_1/spark/task_1/output/part-* \
    /home/cloudera/Desktop/data_samples/avg_rating.csv

head /home/cloudera/Desktop/data_samples/avg_rating.csv -n 10

