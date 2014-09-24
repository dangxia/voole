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

import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Throwables;
import com.voole.hobbit2.storm.order.DynamicPartitionConnections;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;
import com.voole.hobbit2.storm.order.partition.GCSpoutPartition;
import com.voole.hobbit2.storm.order.partition.KafkaSpoutPartition;
import com.voole.hobbit2.storm.order.partition.StormOrderSpoutPartitionCreator;
import com.voole.hobbit2.tools.kafka.KafkaUtils;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class OpaqueTridentKafkaSpout
		implements
		IOpaquePartitionedTridentSpout<List<ISpoutPartition>, ISpoutPartition, Long> {

	@Override
	public Emitter<List<ISpoutPartition>, ISpoutPartition, Long> getEmitter(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutEmitter();
	}

	@Override
	public Coordinator<List<ISpoutPartition>> getCoordinator(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OpaqueTridentKafkaSpoutCoordinator();
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
			IOpaquePartitionedTridentSpout.Emitter<List<ISpoutPartition>, ISpoutPartition, Long> {

		private final DynamicPartitionConnections connections;

		public OpaqueTridentKafkaSpoutEmitter() {
			connections = new DynamicPartitionConnections();
		}

		@Override
		public Long emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, ISpoutPartition partition,
				Long lastPartitionMeta) {
			if (partition instanceof GCSpoutPartition) {
				return emitPartitionBatchGc(tx, collector,
						(GCSpoutPartition) partition, lastPartitionMeta);
			} else {
				try {
					return emitPartitionBatchKafka(tx, collector,
							(KafkaSpoutPartition) partition, lastPartitionMeta);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// TODO
			return null;
		}

		protected Long emitPartitionBatchGc(TransactionAttempt tx,
				TridentCollector collector, GCSpoutPartition partition,
				Long lastPartitionMeta) {
			// TODO
			return null;
		}

		protected Long emitPartitionBatchKafka(TransactionAttempt tx,
				TridentCollector collector, KafkaSpoutPartition partition,
				Long lastPartitionMeta) throws IOException {
			long offset = partition.getOffset();
			if (partition.getNoendPaths() != null
					&& partition.getNoendPaths().size() > 0) {
				// first emit
				for (String pathStr : partition.getNoendPaths()) {
					Path path = new Path(pathStr);
					FileReader<SpecificRecordBase> reader = StormOrderHDFSUtils
							.getNoendReader(path);
					SpecificRecordBase recordBase = null;
					while (reader.hasNext()) {
						recordBase = reader.next(recordBase);
						emit(collector, recordBase);
					}
					reader.close();
				}
				partition.getNoendPaths().clear();
			} else if (lastPartitionMeta != null) {
				offset = lastPartitionMeta;
			}
			SimpleConsumer consumer = connections.register(partition
					.getBrokerAndTopicPartition());
			ByteBufferMessageSet msgs = KafkaUtils.fetch(consumer, partition
					.getBrokerAndTopicPartition().getPartition(), offset + 1,
					StormOrderMetaConfigs.getKafkafetchSize());
			long lastOffset = 0l;
			for (MessageAndOffset msg : msgs) {
				emit(collector, msg);
				lastOffset = msg.offset();
			}
			if (lastOffset == 0l) {
				return offset;
			} else {
				return lastOffset;
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

}
