/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class StormOrderSpoutPartitionFetcher {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderSpoutPartitionFetcher.class);

	private volatile long lastFetchTime = 0;
	private volatile List<KafkaSpoutPartition> cache;

	public StormOrderSpoutPartitionFetcher() {
	}

	public List<KafkaSpoutPartition> fetch() throws FileNotFoundException,
			IOException {
		if (!isShouldReturnCache()) {
			_fetch();
		}
		return cache;
	}

	private void _fetch() throws FileNotFoundException, IOException {
		String[] topics = StormOrderMetaConfigs.getWhiteTopics().toArray(
				new String[] {});
		log.info("fetch SpoutPartition with white topics:"
				+ Joiner.on(',').join(topics));
		List<KafkaSpoutPartition> partitions = getPartitions(topics);
		Collections.sort(partitions);

		lastFetchTime = System.currentTimeMillis();
		cache = partitions;
	}

	private boolean isShouldReturnCache() {
		if (lastFetchTime == 0
				|| System.currentTimeMillis() - lastFetchTime > 60 * 1000
				|| cache == null || cache.size() == 0) {
			return false;
		}
		return true;
	}

	private List<KafkaSpoutPartition> getPartitions(String[] topics)
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

}
