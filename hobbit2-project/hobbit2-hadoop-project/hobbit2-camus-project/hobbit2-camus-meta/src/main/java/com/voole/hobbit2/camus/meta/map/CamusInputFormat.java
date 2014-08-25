/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.map;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartitionState;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusInputFormat extends
		InputFormat<KafkaSplitPartitionState, byte[]> {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		// ZookeeperUtils.createZKClient(zkServers, sessionTimeout,
		// connectionTimeout)
		// KafkaUtils.getPartitionState(zkClient, topics)
		Map<Broker, List<KafkaPartitionState>> map = null;
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordReader<KafkaSplitPartitionState, byte[]> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
