/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.test;

import kafka.api.OffsetRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.function.KafkaTermialOrginToSimpleFunction;
import com.voole.hibbit.storm.kafka.function.KafkaTerminalProtoBuffTransformerFunction;
import com.voole.hibbit.storm.kafka.function.PrintFunction;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hibbit.storm.kafka.test.TestTransformer.LocalShuffle;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class TestTransformer3 {
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

		@Override
		public Integer getFetchSizeBytes() {
			return 1000;
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

		@Override
		public Integer getFetchSizeBytes() {
			return null;
		}
	};

	public interface TransformerConfig {
		int getMaxSpoutPending();

		int getNumWorkers();

		Integer getMaxTaskParallelism();

		Integer getFetchSizeBytes();
	}

	public static TridentTopology createTopology(
			TransformerConfig transformerConfig) {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		String[] topics = new String[] { "t_playbgn_v2", "t_playbgn_v3",
				"t_playend_v2", "t_playend_v3", "t_playalive_v2",
				"t_playalive_v3" };
		KafkaConfig orderKafkaConfig = new KafkaConfig(hosts, topics);
		orderKafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());
		if (transformerConfig.getFetchSizeBytes() != null) {
			orderKafkaConfig.setFetchSizeBytes(transformerConfig
					.getFetchSizeBytes());
		}
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout(
				orderKafkaConfig);

		TridentTopology topology = new TridentTopology();
		topology.newStream("order-kafka-stream", orderKafkaSpout)
				.parallelismHint(topics.length * 4)
				.partition(new LocalShuffle())
				.each(KafkaTerminalProtoBuffTransformerFunction.INPUT_FIELDS,
						new KafkaTerminalProtoBuffTransformerFunction(),
						KafkaTerminalProtoBuffTransformerFunction.OUTPUT_FIELDS)
				.each(KafkaTermialOrginToSimpleFunction.INPUT_FIELDS,
						new KafkaTermialOrginToSimpleFunction(),
						KafkaTermialOrginToSimpleFunction.OUTPUT_FIELDS)
				.each(KafkaTermialOrginToSimpleFunction.OUTPUT_FIELDS,
						new PrintFunction()).parallelismHint(28);
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

	public static void runLocal(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TridentTopology topology = createTopology(localTransformerConfig);
		Config conf = getConfig(localTransformerConfig);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());
	}

	public static void runCluster(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TridentTopology topology = createTopology(clusterTransformerConfig);
		Config conf = getConfig(clusterTransformerConfig);
		StormSubmitter.submitTopology("test-kafka-spout-name", conf,
				topology.build());
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		runLocal(args);
		// cluster(args);
	}

}
