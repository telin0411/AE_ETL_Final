javac -classpath /opt/hbase-0.98.3-hadoop2/lib/hadoop-client-2.2.0.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-common-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-hdfs-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-mapreduce-client-app-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-mapreduce-client-common-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-mapreduce-client-core-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-mapreduce-client-jobclient-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hadoop-mapreduce-client-shuffle-2.4.1.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-common-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-common-0.98.3-hadoop2-tests.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-hadoop2-compat-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-hadoop-compat-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-protocol-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-shell-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/commons-configuration-1.6.jar:/opt/hbase-0.98.3-hadoop2/lib/commons-io-2.4.jar:/opt/hbase-0.98.3-hadoop2/lib/commons-lang-2.6.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-common-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-client-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-server-0.98.3-hadoop2.jar:/opt/hbase-0.98.3-hadoop2/lib/zookeeper-3.4.6.jar:/opt/hbase-0.98.3-hadoop2/lib/htrace-core-2.04.jar:/opt/hbase-0.98.3-hadoop2/lib/hbase-protocol-0.98.3-hadoop2.jar -d classFolder javaFolder/*.java

jar -cvf AE_ETL.jar -C classFolder .