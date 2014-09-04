/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.voole.hobbit2.camus.mr.CamusHDFSUtils;
import com.voole.hobbit2.camus.mr.CamusMetaConfigs;
import com.voole.hobbit2.camus.mr.common.CamusKey;
import com.voole.hobbit2.common.config.KafkaMetaConfigs;
import com.voole.hobbit2.common.config.ZookeeperMetaConfigs;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.ZookeeperUtils;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月2日
 */
public class CamusInputFormat extends
		InputFormat<CamusKey, SpecificRecordBase> {
	private static final Logger log = org.slf4j.LoggerFactory
			.getLogger(CamusInputFormat.class);

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] topics = CamusMetaConfigs.getWhiteTopics(context);
		log.info("camus topics:" + Joiner.on(",").join(topics));
		ZkClient client = ZookeeperUtils.createZKClient(conf
				.get(KafkaMetaConfigs.KAFKA_ZOOKEEPER_CONNECT), conf.getInt(
				ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS, 40000),
				conf.getInt(
						ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
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

		CamusHDFSUtils.writePrevPartionsStates(conf,
				new Path(FileOutputFormat.getOutputPath(context),
						CamusMetaConfigs.REQUESTS_FILE), prevOffsets);

		for (Entry<TopicPartition, Long> entry : prevOffsets.entrySet()) {
			TopicPartition partition = entry.getKey();
			long offset = entry.getValue();
			if (partitionToStateMap.containsKey(partition)) {
				PartitionState state = partitionToStateMap.get(partition);
				log.info(partition + "\toffset start from:" + offset);
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
	public RecordReader<CamusKey, SpecificRecordBase> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new CamusRecordReader(split, context);
	}

}
