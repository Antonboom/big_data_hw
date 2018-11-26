#!/usr/bin/env bash

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_1
hdfs dfs -mkdir -p /user/cloudera/hw_part_1/spark/task_1/data

#hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/samples_100.json \
#                        /user/cloudera/hw_part_1/spark/task_1/data/samples_100.json

hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/Electronics.json \
			 /user/cloudera/hw_part_1/spark/task_1/data/Electronics.json

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_1

