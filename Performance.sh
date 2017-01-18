strategiesChoosing=true
sampleCardinality=1000
sketchTries=500
shufflePartitions=5
output=result
R_0=file:/home/wuxiaoqi/multi-join/twitter.json
R_1=file:/home/wuxiaoqi/multi-join/twitter.json
R_2=file:/home/wuxiaoqi/multi-join/twitter.json


~/multi-join/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.RunBenchmark \
--master spark://wuxiaoqi:7077 \
--executor-cores 1 \
--total-executor-cores 4 \
--driver-library-path /usr/local/lib/jni/ \
--driver-class-path /usr/local/share/java/glpk-java.jar:. \
spark-sql-perf_2.11-0.4.11-SNAPSHOT.jar \
--benchmark MultiJoinPerformance -s ~/multi-join/test-data/single/
