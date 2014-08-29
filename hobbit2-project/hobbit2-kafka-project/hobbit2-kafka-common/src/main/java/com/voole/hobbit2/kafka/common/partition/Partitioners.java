/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.partition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public class Partitioners {
	private final Map<String, Partitioner<?, ?, ?>> topicToPartitionerMap;

	public Partitioners() {
		topicToPartitionerMap = new ConcurrentHashMap<String, Partitioner<?, ?, ?>>();
	}

	public Partitioners(PartitionerRegister... initiators) {
		this();
		for (PartitionerRegister partitionerInitiator : initiators) {
			partitionerInitiator.register(this);
		}
	}

	public void register(String topic, Partitioner<?, ?, ?> partitioner) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
				"topic is empty");
		Preconditions.checkNotNull(partitioner, "partitioner is null");
		topicToPartitionerMap.put(topic, partitioner);
	}

	public void unregister(String topic) {
		topicToPartitionerMap.remove(topic);
	}

	public Partitioner<?, ?, ?> get(String topic) {
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
