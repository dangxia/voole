/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.updater;

import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.function.aggregator.OnlineUserModifierCombiner;
import com.voole.hobbit.storm.order.module.OnlineUser;
import com.voole.hobbit.storm.order.state.OnlineUserState;
import com.voole.hobbit.utils.Tuple;

public class OnlineUserStateQueryUpdater implements
		StateUpdater<OnlineUserState> {
	public static final Fields INPUT_FIELDS = OnlineUserModifierCombiner.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("global");

	public static void sdfsd(Stream stream, TridentState onlineUserState) {
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void updateState(OnlineUserState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			collector.emit(new Values(
					state.update((Map<Tuple<String, Long>, OnlineUser>) tuple
							.get(0))));
		}
	}

}