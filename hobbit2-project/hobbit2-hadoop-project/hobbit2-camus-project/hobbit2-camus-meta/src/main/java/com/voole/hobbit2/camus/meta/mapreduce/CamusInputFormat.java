/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;

import com.voole.hobbit2.camus.meta.CamusHDFSUtils;
import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.config.props.KafkaConfigKeys;
import com.voole.hobbit2.config.props.ZookeeperConfigKeys;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.ZookeeperUtils;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusInputFormat extends InputFormat<CamusKafkaKey, BytesWritable> {
	private static final Logger log = org.slf4j.LoggerFactory
			.getLogger(CamusInputFormat.class);

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] topics = CamusMetaConfigs.getWhiteTopics(context);
		log.info("camus topics:" + topics);
		ZkClient client = ZookeeperUtils.createZKClient(conf
				.get(KafkaConfigKeys.KAFKA_ZOOKEEPER_CONNECT), conf.getInt(
				ZookeeperConfigKeys.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS, 40000),
				conf.getInt(
						ZookeeperConfigKeys.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
						40000));

		Map<Broker, List<PartitionState>> kafkaPartitionStatesMap = KafkaUtils
				.getPartitionState(client, topics);
		client.close();
		Map<TopicPartition, PartitionState> partitionToStateMap = new HashMap<TopicPartition, PartitionState>();
		for (Entry<Broker, List<PartitionState>> entry : kafkaPartitionStatesMap
				.entrySet()) {
			List<PartitionState> partitionStates = entry.getValue();
			for (PartitionState kafkaPartitionState : partitionStates) {
				partitionToStateMap.put(kafkaPartitionState
						.getBrokerAndTopicPartition().getPartition(),
						kafkaPartitionState);
			}
		}
		Map<TopicPartition, Long> prevOffsets = CamusHDFSUtils
				.readMixedPreviousOffsets(conf,
						CamusMetaConfigs.getExecHistoryPath(context));

		for (Entry<TopicPartition, Long> entry : prevOffsets.entrySet()) {
			TopicPartition partition = entry.getKey();
			long offset = entry.getValue();
			if (partitionToStateMap.containsKey(partition)) {
				PartitionState state = partitionToStateMap.get(partition);
				state.setOffset(offset);
				if (state.getEarliestOffset() > state.getOffset()
						|| state.getOffset() > state.getLatestOffset()) {
					if (state.getEarliestOffset() > offset) {
						log.error("The earliest offset was found to be more than the current offset");
						log.error("Moving to the earliest offset available");
					} else {
						log.error("The current offset was found to be more than the latest offset");
						log.error("Moving to the earliest offset available");
					}
					state.setOffset(state.getEarliestOffset());
				}
			}
		}
		return CamusInputSplit.createSplits(kafkaPartitionStatesMap,
				CamusMetaConfigs.getJobMaps(context),
				CamusMetaConfigs.getSplitMinSize(context));
	}

	@Override
	public RecordReader<CamusKafkaKey, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new CamusRecordReader(split, context);
	}

}
