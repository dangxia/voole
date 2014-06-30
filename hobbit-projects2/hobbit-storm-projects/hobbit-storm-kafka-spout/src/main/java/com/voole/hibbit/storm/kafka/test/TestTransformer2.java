/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.test;

import java.util.HashMap;
import java.util.Map;

import kafka.api.OffsetRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hibbit.storm.kafka.test.TestTransformer.LocalShuffle;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;
import com.voole.hobbit.transformer.KafkaTerminalProtoBuffTransformer;

/**
 * @author XuehuiHe
 * @date 2014年6月3日
 */
public class TestTransformer2 {
	private static Logger logger = LoggerFactory
			.getLogger(TestTransformer.class);

	private static TransformerConfig localTransformerConfig = new TransformerConfig() {

		@Override
		public int getNumWorkers() {
			return 1;
		}

		@Override
		public Integer getMaxTaskParallelism() {
			return 1;
		}

		@Override
		public int getMaxSpoutPending() {
			return 2;
		}
	};

	private static TransformerConfig clusterTransformerConfig = new TransformerConfig() {

		@Override
		public int getNumWorkers() {
			return 4;
		}

		@Override
		public Integer getMaxTaskParallelism() {
			return null;
		}

		@Override
		public int getMaxSpoutPending() {
			return 20;
		}
	};

	public interface TransformerConfig {
		int getMaxSpoutPending();

		int getNumWorkers();

		Integer getMaxTaskParallelism();
	}

	public static TridentTopology createTopology() {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		String[] topics = new String[] { "t_playbgn_v2", "t_playbgn_v3",
				"t_playend_v2", "t_playend_v3", "t_playalive_v2",
				"t_playalive_v3" };
		KafkaConfig orderKafkaConfig = new KafkaConfig(hosts, topics);
		orderKafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout(
				orderKafkaConfig);

		TridentTopology topology = new TridentTopology();
		topology.newStream("order-kafka-stream", orderKafkaSpout)
				.parallelismHint(topics.length * 4)
				.partition(new LocalShuffle())
				.each(new Fields("topic", "offset", "partition", "bytes"),
						new TestKafkaFunction(), new Fields())
				.parallelismHint(28);
		return topology;
	}

	public static Config getConfig(TransformerConfig transformerConfig) {
		Config conf = new Config();
		conf.setMaxSpoutPending(transformerConfig.getMaxSpoutPending());
		conf.setNumWorkers(transformerConfig.getNumWorkers());
		if (transformerConfig.getMaxTaskParallelism() != null) {
			conf.setMaxTaskParallelism(transformerConfig
					.getMaxTaskParallelism());
		}

		return conf;
	}

	public static void local(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TridentTopology topology = createTopology();
		Config conf = getConfig(localTransformerConfig);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());
	}

	public static void cluster(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TridentTopology topology = createTopology();
		Config conf = getConfig(clusterTransformerConfig);
		StormSubmitter.submitTopology("test-kafka-spout-name", conf,
				topology.build());
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// local(args);
		cluster(args);
	}

	public static final class TestKafkaFunction extends BaseFunction {
		private Map<String, KafkaTerminalProtoBuffTransformer<?>> transformerMap;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			super.prepare(conf, context);
			transformerMap = new HashMap<String, KafkaTerminalProtoBuffTransformer<?>>();
			try {
				transformerMap
						.put("t_playbgn_v2",
								new KafkaTerminalProtoBuffTransformer<OrderPlayBgnReqV2>(
										OrderPlayBgnReqV2.class));
				transformerMap
						.put("t_playbgn_v3",
								new KafkaTerminalProtoBuffTransformer<OrderPlayBgnReqV3>(
										OrderPlayBgnReqV3.class));
				transformerMap
						.put("t_playend_v2",
								new KafkaTerminalProtoBuffTransformer<OrderPlayEndReqV2>(
										OrderPlayEndReqV2.class));
				transformerMap
						.put("t_playend_v3",
								new KafkaTerminalProtoBuffTransformer<OrderPlayEndReqV3>(
										OrderPlayEndReqV3.class));
				transformerMap
						.put("t_playalive_v2",
								new KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV2>(
										OrderPlayAliveReqV2.class));
				transformerMap
						.put("t_playalive_v3",
								new KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV3>(
										OrderPlayAliveReqV3.class));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			byte[] bytes = tuple.getBinaryByField("bytes");
			long offset = tuple.getLongByField("offset");
			String topic = tuple.getStringByField("topic");
			try {
				if (offset % 10000 == 0) {
					int partition = tuple.getIntegerByField("partition");
					logger.info("topic:" + topic + "\tpartition:" + partition
							+ "\toffset:" + offset);
				}
				transformerMap.get(topic).transform(bytes);
			} catch (RuntimeException e) {
				logger.warn("transformer error:topic\t" + topic + "\tmsg\t"
						+ e.getMessage());
			} catch (Exception e) {
				logger.warn("transformer error", e);
			}
		}
	}
}
