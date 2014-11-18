/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.spout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.avro.specific.SpecificRecordBase;
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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.storm.order.DynamicPartitionConnections;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.storm.order.partition.KafkaSpoutPartition;
import com.voole.hobbit2.storm.order.partition.StormOrderSpoutPartitionFetcher;
import com.voole.hobbit2.storm.order.util.KafkaRecordDehydration;
import com.voole.hobbit2.storm.order.util.TopicMetaManagerUtil;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class OpaqueTridentKafkaSpout
		implements
		IOpaquePartitionedTridentSpout<List<KafkaSpoutPartition>, KafkaSpoutPartition, JSONObject> {
	private static final Logger log = LoggerFactory
			.getLogger(OpaqueTridentKafkaSpout.class);

	@Override
	public Emitter<List<KafkaSpoutPartition>, KafkaSpoutPartition, JSONObject> getEmitter(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutEmitter(conf);
	}

	@Override
	public Coordinator<List<KafkaSpoutPartition>> getCoordinator(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutCoordinator(conf);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("data");
	}

	class OpaqueTridentKafkaSpoutCoordinator
			implements
			IOpaquePartitionedTridentSpout.Coordinator<List<KafkaSpoutPartition>> {
		private final StormOrderSpoutPartitionFetcher spoutPartitionFetcher;

		public OpaqueTridentKafkaSpoutCoordinator(
				@SuppressWarnings("rawtypes") Map conf) {
			spoutPartitionFetcher = new StormOrderSpoutPartitionFetcher();
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public List<KafkaSpoutPartition> getPartitionsForBatch() {
			try {
				return spoutPartitionFetcher.fetch();
			} catch (Exception e) {
				Throwables.propagate(e);
			}
			return null;
		}

		@Override
		public void close() {
		}

	}

	class OpaqueTridentKafkaSpoutEmitter
			implements
			IOpaquePartitionedTridentSpout.Emitter<List<KafkaSpoutPartition>, KafkaSpoutPartition, JSONObject> {

		private final DynamicPartitionConnections connections;
		private final String _topologyName;

		private final @SuppressWarnings("rawtypes") Map conf;

		public OpaqueTridentKafkaSpoutEmitter(
				@SuppressWarnings("rawtypes") Map conf) {
			this.conf = conf;
			connections = new DynamicPartitionConnections();
			_topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		}

		@Override
		public JSONObject emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, KafkaSpoutPartition spoutPartition,
				JSONObject lastPartitionMeta) {
			try {
				TopicPartition topicPartition = spoutPartition
						.getBrokerAndTopicPartition().getPartition();
				// 新的topology
				if (lastPartitionMeta == null
						|| !_topologyName.equals(PartitionMetaUtil
								.getTopologyName(lastPartitionMeta))) {
					lastPartitionMeta = new JSONObject();
					PartitionMetaUtil.setTopologyName(lastPartitionMeta,
							_topologyName);
					// first emit
					log.info("first emit for spout partition:" + topicPartition);
					Optional<Long> foundOffset = StormOrderHDFSUtils
							.findOffset(
									spoutPartition.getBrokerAndTopicPartition(),
									conf);
					// set partition offset
					if (foundOffset.isPresent()) {
						PartitionMetaUtil.setPartitionOffset(lastPartitionMeta,
								foundOffset.get());
						log.info("offset set to:" + foundOffset.get()
								+ " for spout partition:" + topicPartition);
					} else {
						throw new RuntimeException("spout partition:"
								+ topicPartition + " is empty!!");
					}
				}
				long offset = PartitionMetaUtil
						.getPartitionOffset(lastPartitionMeta);

				// log.info(topicPartition + " start fetch from offset:" +
				// offset);

				SimpleConsumer consumer = connections.register(spoutPartition
						.getBrokerAndTopicPartition());
				ByteBufferMessageSet msgs = KafkaUtils.fetch(consumer,
						spoutPartition.getBrokerAndTopicPartition()
								.getPartition(), offset + 1,
						StormOrderMetaConfigs.getKafkafetchSize());
				long lastOffset = -1l;
				long count = 0;
				for (MessageAndOffset msg : msgs) {
					emit(collector, msg, spoutPartition
							.getBrokerAndTopicPartition().getPartition()
							.getTopic());
					lastOffset = msg.offset();
					count++;
				}
				if (count == 0l) {
					// log.info(topicPartition + ", emit count:" + count
					// + ", offset:" + offset);
					return PartitionMetaUtil.newJSONObject(_topologyName,
							offset);
				} else {
					// log.info(topicPartition + ", emit count:" + count
					// + ", offset:" + lastOffset);
					return PartitionMetaUtil.newJSONObject(_topologyName,
							lastOffset);
				}
			} catch (IOException e) {
				log.warn(spoutPartition + " fetch msg failed");
				return lastPartitionMeta;
			}
		}

		private void emit(TridentCollector collector, MessageAndOffset msg,
				String topic) {
			ByteBuffer payload = msg.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);

			try {
				Optional<SpecificRecordBase> target = TopicMetaManagerUtil
						.get().findTopicMeta(topic).getTransformer()
						.transform(bytes);
				if (target.isPresent()) {
					emit(collector, target.get());
				}
			} catch (TransformException e) {
				log.warn("transform failed", e);
			}

		}

		protected void emit(TridentCollector collector,
				SpecificRecordBase recordBase) {
			try {
				KafkaRecordDehydration.dry(recordBase);
				if (recordBase != null) {
					collector.emit(new Values(recordBase));
				}
			} catch (Exception e) {
				log.warn("record dry failed", e);
			}
		}

		@Override
		public void refreshPartitions(
				List<KafkaSpoutPartition> partitionResponsibilities) {
			// log.info("-------------refreshPartitions------------");
			// connections.clear();
		}

		@Override
		public List<KafkaSpoutPartition> getOrderedPartitions(
				List<KafkaSpoutPartition> allPartitionInfo) {
			return allPartitionInfo;
		}

		@Override
		public void close() {
			log.info("-------------close------------");
			connections.clear();
		}

	}

	public static void main(String[] args) {
		PartitionState partitionState = StormOrderHDFSUtils
				.readKafkaPartitionState().entrySet().iterator().next()
				.getValue();
		System.out.println(partitionState);
		DynamicPartitionConnections connections = new DynamicPartitionConnections();
		SimpleConsumer consumer = connections.register(partitionState
				.getBrokerAndTopicPartition());
		ByteBufferMessageSet msgs = KafkaUtils.fetch(consumer, partitionState
				.getBrokerAndTopicPartition().getPartition(), partitionState
				.getLatestOffset() - 100, StormOrderMetaConfigs
				.getKafkafetchSize());
		for (MessageAndOffset msg : msgs) {
			System.out.println(msg.offset());
		}
	}

}
