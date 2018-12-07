#!/usr/bin/env bash

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_2
hdfs dfs -mkdir -p /user/cloudera/hw_part_1/spark/task_2/data

hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/avg_rating.csv \
                        /user/cloudera/hw_part_1/spark/task_2/data/avg_rating.csv

#hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/samples_100_meta.json \
#                        /user/cloudera/hw_part_1/spark/task_2/data/samples_100_meta.json

hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/Electronics_meta.json \
                         /user/cloudera/hw_part_1/spark/task_2/data/Electronics_meta.json

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_2/

