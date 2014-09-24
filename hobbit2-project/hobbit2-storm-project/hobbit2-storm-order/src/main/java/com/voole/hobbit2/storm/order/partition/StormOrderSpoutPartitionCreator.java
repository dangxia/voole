/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ISpoutPartition;

import com.google.common.base.Joiner;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class StormOrderSpoutPartitionCreator {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderSpoutPartitionCreator.class);
	private static final SpoutPartitionComparator COMPARATOR = new SpoutPartitionComparator();

	public StormOrderSpoutPartitionCreator() {
	}

	public static List<ISpoutPartition> create() throws FileNotFoundException,
			IOException {
		String[] topics = StormOrderMetaConfigs.getWhiteTopics().toArray(
				new String[] {});
		log.info("white topics:" + Joiner.on(',').join(topics));
		List<KafkaSpoutPartition> partitions = getPartitions(topics);
		List<ISpoutPartition> result = new ArrayList<ISpoutPartition>();
		result.addAll(partitions);
		result.add(new GCSpoutPartition());
		Collections.sort(result, COMPARATOR);
		return result;
	}

	public static List<KafkaSpoutPartition> getPartitions(String[] topics)
			throws FileNotFoundException, IOException {
		ZkClient client = StormOrderMetaConfigs.createZKClient();
		List<BrokerAndTopicPartition> partitions = KafkaUtils.getPartitions2(
				client, topics);
		client.close();
		List<KafkaSpoutPartition> result = new ArrayList<KafkaSpoutPartition>();
		for (BrokerAndTopicPartition brokerAndTopicPartition : partitions) {
			result.add(new KafkaSpoutPartition(brokerAndTopicPartition));
		}
		return result;
	}

	private static class SpoutPartitionComparator implements
			Comparator<ISpoutPartition> {

		@Override
		public int compare(ISpoutPartition o1, ISpoutPartition o2) {
			if (o1.getClass() != o2.getClass()) {
				if (o1.getClass() == GCSpoutPartition.class) {
					return 1;
				}
				return -1;
			} else {
				return ((KafkaSpoutPartition) o1)
						.compareTo((KafkaSpoutPartition) o2);
			}
		}

	}
}
