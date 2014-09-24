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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.voole.hobbit2.storm.order.DynamicPartitionConnections;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.storm.order.partition.GCSpoutPartition;
import com.voole.hobbit2.storm.order.partition.KafkaSpoutPartition;
import com.voole.hobbit2.storm.order.partition.StormOrderSpoutPartitionCreator;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout.PartitionMeta;
import com.voole.hobbit2.tools.kafka.KafkaUtils;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class OpaqueTridentKafkaSpout
		implements
		IOpaquePartitionedTridentSpout<List<ISpoutPartition>, ISpoutPartition, PartitionMeta> {
	private static final Logger log = LoggerFactory
			.getLogger(OpaqueTridentKafkaSpout.class);

	@Override
	public Emitter<List<ISpoutPartition>, ISpoutPartition, PartitionMeta> getEmitter(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutEmitter(conf);
	}

	@Override
	public Coordinator<List<ISpoutPartition>> getCoordinator(
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

	class OpaqueTridentKafkaSpoutCoordinator implements
			IOpaquePartitionedTridentSpout.Coordinator<List<ISpoutPartition>> {

		public OpaqueTridentKafkaSpoutCoordinator(
				@SuppressWarnings("rawtypes") Map conf) {
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public List<ISpoutPartition> getPartitionsForBatch() {
			try {
				return StormOrderSpoutPartitionCreator.create();
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
			IOpaquePartitionedTridentSpout.Emitter<List<ISpoutPartition>, ISpoutPartition, PartitionMeta> {

		private final DynamicPartitionConnections connections;
		String _topologyName;

		public OpaqueTridentKafkaSpoutEmitter(
				@SuppressWarnings("rawtypes") Map conf) {
			connections = new DynamicPartitionConnections();
			_topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		}

		@Override
		public PartitionMeta emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, ISpoutPartition partition,
				PartitionMeta lastPartitionMeta) {
			try {
				if (partition instanceof GCSpoutPartition) {
					return emitPartitionBatchGc(tx, collector,
							(GCSpoutPartition) partition, lastPartitionMeta);
				} else {
					return emitPartitionBatchKafka(tx, collector,
							(KafkaSpoutPartition) partition, lastPartitionMeta);

				}
			} catch (IOException e) {
				Throwables.propagate(e);
			}
			return null;
		}

		protected PartitionMeta emitPartitionBatchGc(TransactionAttempt tx,
				TridentCollector collector, GCSpoutPartition partition,
				PartitionMeta lastPartitionMeta) {
			// TODO
			return null;
		}

		protected PartitionMeta emitNoend(KafkaSpoutPartition partition,
				PartitionMeta meta, int noendIndex, TridentCollector collector)
				throws IOException {
			List<Path> paths = StormOrderHDFSUtils.getNoendFilePaths(partition
					.getPartition().getTopic());
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
					meta.setNoend(noendIndex + 1);
				} else {
					meta.setNoend(-1);
				}
			}
			return meta;
		}

		protected PartitionMeta emitPartitionBatchKafka(TransactionAttempt tx,
				TridentCollector collector, KafkaSpoutPartition partition,
				PartitionMeta lastPartitionMeta) throws IOException {

			if (lastPartitionMeta == null
					|| !lastPartitionMeta.getTopologyName().equals(
							_topologyName)) {
				PartitionMeta meta = new PartitionMeta();
				meta.setTopologyName(_topologyName);
				// first emit
				log.info("first emit for partition:" + partition);
				// emit noend
				if (partition.getPartition().getPartition() == 0) {
					emitNoend(partition, meta, 0, collector);
				}
				Optional<Long> foundOffset = StormOrderHDFSUtils
						.findOffset(partition);
				if (foundOffset.isPresent()) {
					meta.setOffset(foundOffset.get());
					log.info("offset set to:" + foundOffset.get()
							+ " for partition:" + partition);
					return meta;
				} else {
					throw new RuntimeException("partition:" + partition
							+ " is empty!!");
				}

			} else if (lastPartitionMeta.hasNoend()) {
				return emitNoend(partition, lastPartitionMeta,
						lastPartitionMeta.getNoendIndex(), collector);
			}
			long offset = lastPartitionMeta.getOffset();

			SimpleConsumer consumer = connections.register(partition);
			ByteBufferMessageSet msgs = KafkaUtils.fetch(consumer,
					partition.getPartition(), offset + 1,
					StormOrderMetaConfigs.getKafkafetchSize());
			long lastOffset = 0l;
			for (MessageAndOffset msg : msgs) {
				emit(collector, msg);
				lastOffset = msg.offset();
			}
			if (lastOffset == 0l) {
				return new PartitionMeta(_topologyName, offset);
			} else {
				return new PartitionMeta(_topologyName, offset);
			}
		}

		/**
		 * @param collector
		 * @param msg
		 */
		private void emit(TridentCollector collector, MessageAndOffset msg) {
			ByteBuffer payload = msg.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);

			collector.emit(new Values(bytes));

		}

		protected void emit(TridentCollector collector,
				SpecificRecordBase recordBase) {
			collector.emit(new Values(recordBase));
		}

		@Override
		public void refreshPartitions(
				List<ISpoutPartition> partitionResponsibilities) {
			connections.clear();
		}

		@Override
		public List<ISpoutPartition> getOrderedPartitions(
				List<ISpoutPartition> allPartitionInfo) {
			return allPartitionInfo;
		}

		@Override
		public void close() {
			connections.clear();
		}

	}

	public static class PartitionMeta extends HashMap<String, Object> {

		public PartitionMeta() {
		}

		public PartitionMeta(String topologyName, long offset) {
			setTopologyName(topologyName);
			setOffset(offset);
		}

		public boolean hasNoend() {
			Integer hasNoend = (Integer) this.get("hasNoend");
			return hasNoend != null && hasNoend != -1;
		}

		public int getNoendIndex() {
			return (Integer) this.get("hasNoend");
		}

		public void setNoend(int noendIndex) {
			if (noendIndex == -1) {
				this.remove(noendIndex);
			} else {
				this.put("hasNoend", noendIndex);
			}
		}

		public void setTopologyName(String topologyName) {
			this.put("topologyName", topologyName);
		}

		public String getTopologyName() {
			return (String) this.get("topologyName");
		}

		public void setOffset(long offset) {
			this.put("offset", offset);
		}

		public Long getOffset() {
			return (Long) this.get("offset");
		}
	}

}
