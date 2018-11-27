#!/usr/bin/env bash

set -e

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON_OPTS=""

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_2/output

#spark2-submit --master yarn product_name_rating.py  \
#    hdfs:///user/cloudera/hw_part_1/spark/task_2/data/avg_rating.csv \
#    hdfs:///user/cloudera/hw_part_1/spark/task_2/data/samples_100_meta.json \
#    hdfs:///user/cloudera/hw_part_1/spark/task_2/output

spark2-submit --master yarn product_name_rating.py  \
    hdfs:///user/cloudera/hw_part_1/spark/task_2/data/avg_rating.csv \
    hdfs:///user/cloudera/hw_part_1/spark/task_2/data/Electronics_meta.json \
    hdfs:///user/cloudera/hw_part_1/spark/task_2/output

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_2/output

hadoop fs -getmerge \
    /user/cloudera/hw_part_1/spark/task_2/output/part-* \
    /home/cloudera/Desktop/data_samples/prodname_avg_rating.csv

head /home/cloudera/Desktop/data_samples/prodname_avg_rating.csv -n 10

