#!/usr/bin/env bash

hdfs dfs -rm -r -f -skipTrash /user/cloudera/hw_part_1/spark/task_3
hdfs dfs -mkdir -p /user/cloudera/hw_part_1/spark/task_3/data

hdfs dfs -copyFromLocal /home/cloudera/Desktop/data_samples/prodname_avg_rating.csv \
			/user/cloudera/hw_part_1/spark/task_3/data/prodname_avg_rating.csv

hdfs dfs -ls -R /user/cloudera/hw_part_1/spark/task_3

