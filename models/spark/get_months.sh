#!/bin/bash
#for i in 06 07 08 09 10 11 12;
for i in 01 02 03 04 05;
do
spark-submit \
    --conf spark.driver.port=5001 \
    --conf spark.blockManager.port=5101 \
    --conf spark.ui.port=5201 \
    --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar' \
    --packages com.databricks:spark-avro_2.11:4.0.0 \
    $@ 2019 $i;
done
