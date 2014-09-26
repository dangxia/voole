/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.spout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;
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
import com.voole.hobbit2.storm.order.util.DryGenerator;
import com.voole.hobbit2.storm.order.util.TopicMetaManagerUtil;
import com.voole.hobbit2.tools.kafka.KafkaUtils;

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
		String _topologyName;

		public OpaqueTridentKafkaSpoutEmitter(
				@SuppressWarnings("rawtypes") Map conf) {
			connections = new DynamicPartitionConnections();
			_topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		}

		@Override
		public JSONObject emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, KafkaSpoutPartition spoutPartition,
				JSONObject lastPartitionMeta) {
			try {
				if (lastPartitionMeta == null
						|| !PartitionMetaUtil.getTopologyName(lastPartitionMeta)
								.equals(_topologyName)) {
					JSONObject meta = new JSONObject();
					PartitionMetaUtil.setTopologyName(meta, _topologyName);
					// first emit
					log.info("first emit for spout partition:" + spoutPartition);
					// emit noend
					if (spoutPartition.getBrokerAndTopicPartition()
							.getPartition().getPartition() == 0) {
						emitNoend(spoutPartition, meta, 0, collector);
					}
					Optional<Long> foundOffset = StormOrderHDFSUtils
							.findOffset(spoutPartition
									.getBrokerAndTopicPartition());
					if (foundOffset.isPresent()) {
						PartitionMetaUtil.setOffset(meta, foundOffset.get());
						log.info("offset set to:" + foundOffset.get()
								+ " for spout partition:" + spoutPartition);
						return meta;
					} else {
						throw new RuntimeException("spout partition:"
								+ spoutPartition + " is empty!!");
					}

				} else if (PartitionMetaUtil.hasNoend(lastPartitionMeta)) {
					return emitNoend(spoutPartition, lastPartitionMeta,
							PartitionMetaUtil.getNoendIndex(lastPartitionMeta),
							collector);
				}
				long offset = PartitionMetaUtil.getOffset(lastPartitionMeta);

				SimpleConsumer consumer = connections.register(spoutPartition
						.getBrokerAndTopicPartition());
				ByteBufferMessageSet msgs = KafkaUtils.fetch(consumer,
						spoutPartition.getBrokerAndTopicPartition()
								.getPartition(), offset + 1,
						StormOrderMetaConfigs.getKafkafetchSize());
				long lastOffset = 0l;
				for (MessageAndOffset msg : msgs) {
					emit(collector, msg, spoutPartition
							.getBrokerAndTopicPartition().getPartition()
							.getTopic());
					lastOffset = msg.offset();
				}
				if (lastOffset == 0l) {

					return PartitionMetaUtil.newJSONObject(_topologyName, offset);
				} else {
					return PartitionMetaUtil.newJSONObject(_topologyName,
							lastOffset);
				}
			} catch (IOException e) {
				Throwables.propagate(e);
			}
			return null;
		}

		protected JSONObject emitNoend(KafkaSpoutPartition partition,
				JSONObject meta, int noendIndex, TridentCollector collector)
				throws IOException {
			List<Path> paths = StormOrderHDFSUtils.getNoendFilePaths(partition
					.getBrokerAndTopicPartition().getPartition().getTopic());
			if (paths != null && paths.size() > noendIndex) {
				Path path = paths.get(noendIndex);
				FileReader<SpecificRecordBase> reader = StormOrderHDFSUtils
						.getNoendReader(path);
				log.info("read noend records for partition:" + partition
						+ ", index:" + noendIndex + " file:"
						+ path.toUri().getPath());
				SpecificRecordBase recordBase = null;
				while (reader.hasNext()) {
					recordBase = reader.next();
					emit(collector, recordBase);
				}
				reader.close();
				if (paths.size() > noendIndex + 1) {
					PartitionMetaUtil.setNoend(meta, noendIndex + 1);
				} else {
					PartitionMetaUtil.setNoend(meta, -1);
				}
			}
			return meta;
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
//				SpecificRecordBase dry = DryGenerator.dry(recordBase);
//				if (dry != null) {
//					collector.emit(new Values(dry));
//				}
				collector.emit(new Values(recordBase));
			} catch (Exception e) {
				log.warn("record dry failed", e);
			}
		}

		@Override
		public void refreshPartitions(
				List<KafkaSpoutPartition> partitionResponsibilities) {
			connections.clear();
		}

		@Override
		public List<KafkaSpoutPartition> getOrderedPartitions(
				List<KafkaSpoutPartition> allPartitionInfo) {
			return allPartitionInfo;
		}

		@Override
		public void close() {
			connections.clear();
		}

	}

	public static class PartitionMetaUtil extends HashMap<String, Object> {

		public static JSONObject newJSONObject(String topologyName, long offset) {
			JSONObject meta = new JSONObject();
			setTopologyName(meta, topologyName);
			setOffset(meta, offset);
			return meta;
		}

		public static boolean hasNoend(JSONObject meta) {
			Number hasNoend = (Number) meta.get("hasNoend");
			return hasNoend != null && hasNoend.intValue() != -1;
		}

		public static int getNoendIndex(JSONObject meta) {
			return ((Number) meta.get("hasNoend")).intValue();
		}

		@SuppressWarnings("unchecked")
		public static void setNoend(JSONObject meta, int noendIndex) {
			if (noendIndex == -1) {
				meta.remove("hasNoend");
			} else {
				meta.put("hasNoend", noendIndex);
			}
		}

		@SuppressWarnings("unchecked")
		public static void setTopologyName(JSONObject meta, String topologyName) {
			meta.put("topologyName", topologyName);
		}

		public static String getTopologyName(JSONObject meta) {
			return (String) meta.get("topologyName");
		}

		@SuppressWarnings("unchecked")
		public static void setOffset(JSONObject meta, long offset) {
			meta.put("offset", offset);
		}

		public static Long getOffset(JSONObject meta) {
			return (Long) meta.get("offset");
		}
	}

}
