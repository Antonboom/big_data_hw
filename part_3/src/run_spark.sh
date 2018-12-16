#!/usr/bin/env bash

source /opt/rh/rh-python36/enable

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON_OPTS=""
spark2-submit --master local[2] $1

