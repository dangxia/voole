/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.consumer.SimpleConsumer;

import com.voole.hibbit.storm.kafka.partition.HostPort;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hibbit.storm.kafka.partition.KafkaPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class DynamicPartitionConnections {
	static class ConnectionInfo {
		SimpleConsumer consumer;
		Set<Integer> partitions = new HashSet<Integer>();

		public ConnectionInfo(SimpleConsumer consumer) {
			this.consumer = consumer;
		}
	}

	Map<HostPort, ConnectionInfo> _connections = new HashMap<HostPort, ConnectionInfo>();
	KafkaConfig _config;

	public DynamicPartitionConnections(KafkaConfig config) {
		_config = config;
	}

	public SimpleConsumer register(KafkaPartition partition) {
		return register(partition.getHostport(), partition.getPartition());
	}

	public SimpleConsumer register(HostPort host, int partition) {
		if (!_connections.containsKey(host)) {
			_connections.put(
					host,
					new ConnectionInfo(new SimpleConsumer(host.getHost(), host
							.getPort(), _config.getSocketTimeoutMs(), _config
							.getBufferSizeBytes(), "")));
		}
		ConnectionInfo info = _connections.get(host);
		info.partitions.add(partition);
		return info.consumer;
	}

	public SimpleConsumer getConnection(KafkaPartition partition) {
		ConnectionInfo info = _connections.get(partition.getHostport());
		if (info != null)
			return info.consumer;
		return null;
	}

	public void unregister(HostPort port, int partition) {
		ConnectionInfo info = _connections.get(port);
		info.partitions.remove(partition);
		if (info.partitions.isEmpty()) {
			info.consumer.close();
			_connections.remove(port);
		}
	}

	public void unregister(KafkaPartition partition) {
		unregister(partition.getHostport(), partition.getPartition());
	}

	public void clear() {
		for (ConnectionInfo info : _connections.values()) {
			info.consumer.close();
		}
		_connections.clear();
	}
}
