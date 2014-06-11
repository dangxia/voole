/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import com.voole.hobbit.cache.AbstractRefreshState.StateRefreshQueryFunction;
import com.voole.hobbit.cache.state.AreaInfoCacheState.AreaInfoCacheStateFactory;

/**
 * @author XuehuiHe
 * @date 2014年6月11日
 */
public class TestStae {
	public static void main(String[] args) {
		Config config = new Config();
		config.setMaxSpoutPending(1);
		config.setMaxTaskParallelism(1);
		config.setNumWorkers(1);

		TridentTopology topology = new TridentTopology();
		TridentState areaInfoState = topology
				.newStaticState(new AreaInfoCacheStateFactory());
		topology.newStream("test-state", new RefreshCmdSender()).stateQuery(
				areaInfoState, StateRefreshQueryFunction.INPUT_FIELDS,
				new StateRefreshQueryFunction(),
				StateRefreshQueryFunction.OUTPUT_FIELDS);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("jsdlfjsljd", config, topology.build());

	}
}
