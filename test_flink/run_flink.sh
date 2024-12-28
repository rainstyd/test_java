
# 服务器本地flink单点集群运行
bin/flink run -c flink.datastream.GetKafkaToKafka rainsty/test_flink-1.0-SNAPSHOT.jar --config=rainsty/test_flink.properties

#Yarn提交运行(只支持在/data/deploy/flink-1.17.1目录执行或者在代码jar包目录执行)
bin/flink run-application -t yarn-application \
-Dyarn.application.name=test_flink \
-Dtaskmanager.memory.process.size=1024M \
-Dtaskmanager.numberOfTaskSlots=1 \
-Dparallelism.default=1 \
-Dyarn.ship-files=rainsty \
-c flink.datastream.GetKafkaToKafka rainsty/test_flink-1.0-SNAPSHOT.jar \
--config=rainsty/test_flink.properties
