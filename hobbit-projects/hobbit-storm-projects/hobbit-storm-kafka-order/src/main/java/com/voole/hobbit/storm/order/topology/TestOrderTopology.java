/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.storm.order.state.PlayExtraStateImpl.PlayExtraUpdater;
import com.voole.hobbit.storm.order.state.updater.OnlineUserStateQueryUpdater;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class TestOrderTopology {
	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory
			.getLogger(TestOrderTopology.class);

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

	public interface TransformerConfig {
		int getMaxSpoutPending();

		int getNumWorkers();

		Integer getMaxTaskParallelism();

		Integer getFetchSizeBytes();
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

	public static TridentTopology createTopology(
			TransformerConfig transformerConfig) {
		TridentTopology topology = new TridentTopology();
		OrderTopology2 orderBgnTopology = new OrderTopology2();
		if (transformerConfig.getFetchSizeBytes() != null) {
			orderBgnTopology.getKafkaConfig().setFetchSizeBytes(
					transformerConfig.getFetchSizeBytes());
		}
		Stream orderBgnStream = orderBgnTopology.build(topology);
		orderBgnStream.each(OnlineUserStateQueryUpdater.OUTPUT_FIELDS,
				new Print());
		return topology;
	}

	public static class Print extends BaseFilter {
		private Gson gson;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
			GsonBuilder gb = new GsonBuilder();
			gb.setPrettyPrinting();
			gson = gb.create();
			super.prepare(conf, context);
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println(gson.toJson(tuple));
			return true;
		}

	}

	public static void main(String[] args) {
		TridentTopology topology = createTopology(localTransformerConfig);
		Config conf = getConfig(localTransformerConfig);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());
	}
}
