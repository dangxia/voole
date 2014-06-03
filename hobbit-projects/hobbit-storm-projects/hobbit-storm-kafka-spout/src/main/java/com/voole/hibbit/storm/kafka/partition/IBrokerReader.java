/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.partition;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.util.KafkaUtils;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public interface IBrokerReader extends Serializable {
	List<KafkaPartition> getCurrentBrokers();

	void close();

	public static class BrokerReaderFactory {
		public static IBrokerReader get(Map<String, Object> conf,
				KafkaConfig kafkaConfig) {
			if (kafkaConfig.getHosts() != null
					&& kafkaConfig.getHosts() instanceof ZkHosts) {
				return new ZkBrokerReader(conf, kafkaConfig,
						(ZkHosts) kafkaConfig.getHosts());
			}
			return null;
		}
	}

	public static class UTF8StringZkSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			if (data != null) {
				return data.toString().getBytes();
			}
			return null;
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes != null) {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new ZkMarshallingError(e);
				}
			}
			return null;
		}

	}

	public static class ZkBrokerReader implements IBrokerReader {
		private final ZkClient zkClient;
		private List<KafkaPartition> cachedBrokers;
		private long lastRefreshTimeMs;
		private long refreshMillis;
		private String topic;

		public ZkBrokerReader(Map<String, Object> conf,
				KafkaConfig kafkaConfig, ZkHosts hosts) {
			this.topic = kafkaConfig.getTopic();
			this.refreshMillis = hosts.getRefreshFreqSecs() * 1000L;
			this.zkClient = new ZkClient(hosts.getKafkaConnetion(),
					getSessionTimeout(conf, hosts), getConnectionTimeout(conf,
							hosts), new UTF8StringZkSerializer());
		}

		public int getConnectionTimeout(Map<String, Object> conf, ZkHosts hosts) {
			Object timeout = null;
			if (hosts.getConnectionTimeout() != null) {
				timeout = hosts.getConnectionTimeout();
			} else {
				timeout = conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT);
			}
			return Utils.getInt(timeout);
		}

		public int getSessionTimeout(Map<String, Object> conf, ZkHosts hosts) {
			Object sessionTimeout = null;
			if (hosts.getSessionTimeout() != null) {
				sessionTimeout = hosts.getSessionTimeout();
			} else {
				sessionTimeout = conf
						.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT);
			}
			return Utils.getInt(sessionTimeout);
		}

		@Override
		public List<KafkaPartition> getCurrentBrokers() {
			long currTime = System.currentTimeMillis();
			if (currTime > lastRefreshTimeMs + refreshMillis) {
				cachedBrokers = KafkaUtils.getPartitions(zkClient, topic);
				lastRefreshTimeMs = currTime;
			}
			return cachedBrokers;
		}

		@Override
		public void close() {
			zkClient.close();
		}

	}
}
