/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitAreaInfoStateFactory;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitOemInfoStateFactory;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitResourceInfoStateFactory;
import com.voole.hobbit.storm.order.OrderOnlineUserState.OrderOnlineUserStateFactory;
import com.voole.hobbit.storm.order.OrderOnlineUserState.OrderOnlineUserStateUpdateQueryFunction;
import com.voole.hobbit.storm.order.OrderSessionState.OrderSessionStateFactory;
import com.voole.hobbit.storm.order.OrderSessionState.OrderSessionStateUpdater;
import com.voole.hobbit.storm.order.function.AreaInfoQueryFunction;
import com.voole.hobbit.storm.order.function.AssemblyOrderSession;
import com.voole.hobbit.storm.order.function.OrderOnlineUserModifierCombinerAggregator;
import com.voole.hobbit.storm.order.function.ResourceQueryFunction;
import com.voole.hobbit.storm.order.function.SpidQueryFunction;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class TestOrderTopology {
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

		TridentState oemInfoCacheState = topology
				.newStaticState(new HobbitOemInfoStateFactory());
		TridentState areaInfoState = topology
				.newStaticState(new HobbitAreaInfoStateFactory());
		TridentState resourceInfoState = topology
				.newStaticState(new HobbitResourceInfoStateFactory());
		TridentState orderOnlineUserState = topology
				.newStaticState(new OrderOnlineUserStateFactory());
		OrderBgnTopology orderBgnTopology = new OrderBgnTopology();
		if (transformerConfig.getFetchSizeBytes() != null) {
			orderBgnTopology.getKafkaConfig().setFetchSizeBytes(
					transformerConfig.getFetchSizeBytes());
		}
		Stream orderBgnStream = orderBgnTopology.build(topology);
		orderBgnStream
				.stateQuery(oemInfoCacheState, SpidQueryFunction.INPUT_FIELDS,
						new SpidQueryFunction(),
						SpidQueryFunction.OUTPUT_FIELDS)
				.stateQuery(areaInfoState, AreaInfoQueryFunction.INPUT_FIELDS,
						new AreaInfoQueryFunction(),
						AreaInfoQueryFunction.OUTPUT_FIELDS)
				.stateQuery(resourceInfoState,
						ResourceQueryFunction.INPUT_FIELDS,
						new ResourceQueryFunction(),
						ResourceQueryFunction.OUTPUT_FIELDS)
				.each(AssemblyOrderSession.INPUT_FIELDS,
						new AssemblyOrderSession(),
						AssemblyOrderSession.OUTPUT_FIELDS)
				.project(AssemblyOrderSession.OUTPUT_FIELDS)
				.partitionBy(new Fields("sessionId"))
				.partitionPersist(new OrderSessionStateFactory(),
						AssemblyOrderSession.OUTPUT_FIELDS,
						new OrderSessionStateUpdater(),
						OrderSessionStateUpdater.OUTPUT_FIELDS)
				.newValuesStream()
				.aggregate(
						OrderOnlineUserModifierCombinerAggregator.INPUT_FIELDS,
						new OrderOnlineUserModifierCombinerAggregator(),
						OrderOnlineUserModifierCombinerAggregator.OUTPUT_FIELDS)
				.stateQuery(orderOnlineUserState,
						OrderOnlineUserStateUpdateQueryFunction.INPUT_FIELDS,
						new OrderOnlineUserStateUpdateQueryFunction(),
						OrderOnlineUserStateUpdateQueryFunction.OUTPUT_FIELDS)
				.each(OrderOnlineUserStateUpdateQueryFunction.OUTPUT_FIELDS,
						new Print());

		//
		// topology.newStream("refresh-all", new RefreshCmdSender())
		// .broadcast()
		// .stateQuery(oemInfoCacheState,
		// StateRefreshQueryFunction.INPUT_FIELDS,
		// new StateRefreshQueryFunction(),
		// StateRefreshQueryFunction.OUTPUT_FIELDS);
		return topology;
	}

	public static class Print extends BaseFilter {
		private Gson gson;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
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
