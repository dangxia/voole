/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ISpoutPartition;

import com.google.common.base.Joiner;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class StormOrderSpoutPartitionCreator {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderSpoutPartitionCreator.class);

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		List<ISpoutPartition> list = StormOrderSpoutPartitionCreator
				.create(false);
		for (ISpoutPartition iSpoutPartition : list) {
			System.out.println(iSpoutPartition);
		}
	}

	public static List<ISpoutPartition> create(boolean loadNoend)
			throws FileNotFoundException, IOException {
		String[] topics = StormOrderMetaConfigs.getWhiteTopics().toArray(
				new String[] {});
		log.info("white topics:" + Joiner.on(',').join(topics));
		List<KafkaSpoutPartition> partitions = getPartitions(topics);
		if (loadNoend) {
			loadNoend(partitions);
		}

		List<ISpoutPartition> result = new ArrayList<ISpoutPartition>();
		result.addAll(partitions);
		result.add(new GCSpoutPartition());
		return result;
	}

	private static void loadNoend(List<KafkaSpoutPartition> partitions)
			throws FileNotFoundException, IOException {
		List<Path> noendPaths = StormOrderHDFSUtils.getNoendFilePaths();
		if (noendPaths.size() == 0 || partitions.size() == 0) {
			return;
		}
		Iterator<Path> noendPathIterator = noendPaths.iterator();
		Iterator<KafkaSpoutPartition> partitionIterator = partitions.iterator();
		while (noendPathIterator.hasNext()) {
			if (!partitionIterator.hasNext()) {
				partitionIterator = partitions.iterator();
			}
			partitionIterator.next().getNoendPaths()
					.add(noendPathIterator.next());
		}
	}

	public static List<ISpoutPartition> create() throws FileNotFoundException,
			IOException {
		return create(true);
	}

	public static List<KafkaSpoutPartition> getPartitions(String[] topics)
			throws FileNotFoundException, IOException {
		ZkClient client = StormOrderMetaConfigs.createZKClient();
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

		Map<TopicPartition, Long> prevOffsets = StormOrderHDFSUtils
				.readMixedPreviousOffsets();

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
		List<KafkaSpoutPartition> result = new ArrayList<KafkaSpoutPartition>();
		for (List<PartitionState> list : kafkaPartitionStatesMap.values()) {
			for (PartitionState partitionState : list) {
				KafkaSpoutPartition partition = new KafkaSpoutPartition();
				partition.setBrokerAndTopicPartition(partitionState
						.getBrokerAndTopicPartition());
				partition.setOffset(partitionState.getOffset());

				result.add(partition);
			}
		}
		return result;
	}
}
