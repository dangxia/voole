/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.consumer.SimpleConsumer;

import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class DynamicPartitionConnections {
	static class ConnectionInfo {
		SimpleConsumer consumer;
		Set<TopicPartition> partitions = new HashSet<TopicPartition>();

		public ConnectionInfo(SimpleConsumer consumer) {
			this.consumer = consumer;
		}
	}

	Map<Broker, ConnectionInfo> _connections = new HashMap<Broker, ConnectionInfo>();

	public DynamicPartitionConnections() {
	}

	public SimpleConsumer register(BrokerAndTopicPartition partition) {
		return register(partition.getBroker(), partition.getPartition());
	}

	public SimpleConsumer register(Broker broker, TopicPartition partition) {
		if (!_connections.containsKey(broker)) {
			_connections.put(
					broker,
					new ConnectionInfo(StormOrderMetaConfigs
							.createSimpleConsumer(broker)));
		}
		ConnectionInfo info = _connections.get(broker);
		info.partitions.add(partition);
		return info.consumer;
	}

	public SimpleConsumer getConnection(BrokerAndTopicPartition partition) {
		ConnectionInfo info = _connections.get(partition.getBroker());
		if (info != null)
			return info.consumer;
		return null;
	}

	public void unregister(BrokerAndTopicPartition partition) {
		Broker broker = partition.getBroker();
		ConnectionInfo info = _connections.get(broker);
		info.partitions.remove(partition.getPartition());
		if (info.partitions.isEmpty()) {
			info.consumer.close();
			_connections.remove(broker);
		}
	}

	public void clear() {
		for (ConnectionInfo info : _connections.values()) {
			info.consumer.close();
		}
		_connections.clear();
	}
}
