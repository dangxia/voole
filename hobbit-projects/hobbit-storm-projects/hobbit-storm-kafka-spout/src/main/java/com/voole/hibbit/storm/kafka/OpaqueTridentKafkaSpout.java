/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka;

import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hibbit.storm.kafka.exception.FailedFetchException;
import com.voole.hibbit.storm.kafka.partition.IBrokerReader;
import com.voole.hibbit.storm.kafka.partition.IBrokerReader.BrokerReaderFactory;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hibbit.storm.kafka.partition.KafkaPartition;
import com.voole.hibbit.storm.kafka.util.KafkaUtils;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
@SuppressWarnings({ "rawtypes" })
public class OpaqueTridentKafkaSpout
		implements
		IOpaquePartitionedTridentSpout<List<KafkaPartition>, KafkaPartition, JSONObject> {

	public static final Logger LOG = LoggerFactory
			.getLogger(OpaqueTridentKafkaSpout.class);

	private final String[] topics;
	private final KafkaConfig kafkaConfig;
	private String _topologyInstanceId = UUID.randomUUID().toString();

	public OpaqueTridentKafkaSpout(KafkaConfig kafkaConfig) {
		this.topics = kafkaConfig.getTopics();
		this.kafkaConfig = kafkaConfig;
	}

	public String[] getTopics() {
		return topics;
	}

	@Override
	public Emitter<List<KafkaPartition>, KafkaPartition, JSONObject> getEmitter(
			Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutEmitter(conf);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutCoordinator(conf);
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("topic", "offset", "partition", "bytes");
	}

	class OpaqueTridentKafkaSpoutCoordinator implements
			IOpaquePartitionedTridentSpout.Coordinator<List<KafkaPartition>> {
		private IBrokerReader reader;

		public OpaqueTridentKafkaSpoutCoordinator(Map<String, Object> conf) {
			reader = BrokerReaderFactory.get(conf, kafkaConfig);
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public List<KafkaPartition> getPartitionsForBatch() {
			return reader.getCurrentBrokers();
		}

		@Override
		public void close() {
			reader.close();
		}
	}

	class OpaqueTridentKafkaSpoutEmitter
			implements
			IOpaquePartitionedTridentSpout.Emitter<List<KafkaPartition>, KafkaPartition, JSONObject> {
		private DynamicPartitionConnections connections;
		String _topologyName;

		public OpaqueTridentKafkaSpoutEmitter(Map conf) {
			connections = new DynamicPartitionConnections(kafkaConfig);
			_topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		}

		@Override
		public JSONObject emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, KafkaPartition partition,
				JSONObject lastMeta) {

			try {
				SimpleConsumer consumer = connections.register(partition);
				JSONObject ret = emitPartitionBatchNew(consumer, partition,
						collector, lastMeta);
				return ret;
			} catch (FailedFetchException e) {
				LOG.warn("Failed to fetch from partition " + partition);
				if (lastMeta == null) {
					return null;
				} else {
					long nextOffset = 0;
					nextOffset = Long.parseLong(lastMeta.get("nextOffset")
							.toString());
					return getMeta(nextOffset, nextOffset, partition);
				}
			}
		}

		@Override
		public void refreshPartitions(
				List<KafkaPartition> partitionResponsibilities) {
			connections.clear();
		}

		@Override
		public List<KafkaPartition> getOrderedPartitions(
				List<KafkaPartition> allPartitionInfo) {
			return allPartitionInfo;
		}

		@Override
		public void close() {
			connections.clear();
		}

		/**
		 * @param consumer
		 * @param partition
		 * @param collector
		 * @param lastMeta
		 * @param _topologyName
		 * @return
		 */
		public JSONObject emitPartitionBatchNew(SimpleConsumer consumer,
				KafkaPartition partition, TridentCollector collector,
				JSONObject lastMeta) throws FailedFetchException {
			long offset;
			if (lastMeta != null) {
				String lastInstanceId = null;
				Map lastTopoMeta = (Map) lastMeta.get("topology");
				if (lastTopoMeta != null) {
					lastInstanceId = (String) lastTopoMeta.get("id");
				}

				if (kafkaConfig.isForceFromStart()
						&& !_topologyInstanceId.equals(lastInstanceId)) {
					offset = KafkaUtils.getOffsetsBefore(consumer, partition,
							kafkaConfig.getStartOffsetTime());
					offset -= 100000;
					if (offset < 0) {
						offset = 0;
					}
				} else {
					offset = (Long) lastMeta.get("nextOffset");
				}
			} else {
				long startTime = -1;
				if (kafkaConfig.isForceFromStart())
					startTime = kafkaConfig.getStartOffsetTime();
				offset = KafkaUtils.getOffsetsBefore(consumer, partition,
						startTime);
				offset -= 100000;
				if (offset < 0) {
					offset = 0;
				}
			}
			ByteBufferMessageSet msgs;
			try {
				msgs = KafkaUtils.fetch(consumer, partition, offset,
						kafkaConfig.getFetchSizeBytes());
			} catch (Exception e) {
				if (e instanceof ConnectException) {
					throw new FailedFetchException(e);
				} else {
					throw new RuntimeException(e);
				}
			}
			long endoffset = offset;
			for (MessageAndOffset msg : msgs) {
				emit(partition, collector, msg);
				endoffset = msg.nextOffset();
			}
			return getMeta(offset, endoffset, partition);
		}

		@SuppressWarnings("unchecked")
		public JSONObject getMeta(long offset, long nextOffset,
				KafkaPartition partition) {
			JSONObject newMeta = new JSONObject();

			JSONObject broker = new JSONObject();
			broker.put("host", partition.getHostport().getHost());
			broker.put("port", partition.getHostport().getPort());
			newMeta.put("broker", broker);

			JSONObject topology = new JSONObject();
			topology.put("name", _topologyName);
			topology.put("id", _topologyInstanceId);
			newMeta.put("topology", topology);

			newMeta.put("offset", offset);
			newMeta.put("nextOffset", nextOffset);
			newMeta.put("instanceId", _topologyInstanceId);
			newMeta.put("partition", partition.getPartition());
			newMeta.put("topic", kafkaConfig.getTopics());
			return newMeta;
		}

		/**
		 * @param collector
		 * @param message
		 */
		private void emit(KafkaPartition partition, TridentCollector collector,
				MessageAndOffset msg) {
			ByteBuffer payload = msg.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);

			collector.emit(new Values(partition.getTopic(), msg.offset(),
					partition.getPartition(), bytes));
		}
	}

}
