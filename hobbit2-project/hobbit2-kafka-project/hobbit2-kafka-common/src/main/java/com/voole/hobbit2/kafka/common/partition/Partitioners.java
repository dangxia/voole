/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.partition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.voole.hobbit2.kafka.common.IKafkaKey;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public class Partitioners {
	private final Map<String, Partitioner<? extends IKafkaKey, ?>> topicToPartitionerMap;

	public Partitioners() {
		topicToPartitionerMap = new ConcurrentHashMap<String, Partitioner<? extends IKafkaKey, ?>>();
	}

	public Partitioners(PartitionerRegister... initiators) {
		this();
		for (PartitionerRegister partitionerInitiator : initiators) {
			partitionerInitiator.register(this);
		}
	}

	public void register(String topic,
			Partitioner<? extends IKafkaKey, ?> partitioner) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
				"topic is empty");
		Preconditions.checkNotNull(partitioner, "partitioner is null");
		topicToPartitionerMap.put(topic, partitioner);
	}

	public void unregister(String topic) {
		topicToPartitionerMap.remove(topic);
	}

	public Partitioner<? extends IKafkaKey, ?> get(String topic) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
				"topic is empty");
		if (topicToPartitionerMap.containsKey(topic)) {
			return topicToPartitionerMap.get(topic);
		} else {
			throw new RuntimeException("topic:" + topic
					+ " Partitioner not found");
		}
	}
}
