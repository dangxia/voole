package com.voole.hobbit2.storm.order2.partition;

public class HdfsKafkaMixedSpoutPartitionFetcher {
	public static enum State {
		HDFS_NOEND, KAFKA_HISTORY, KAFKA_REALTIME;
	}
}
