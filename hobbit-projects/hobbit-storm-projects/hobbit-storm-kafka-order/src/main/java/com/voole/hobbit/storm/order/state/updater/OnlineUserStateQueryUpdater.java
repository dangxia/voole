/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.updater;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.function.aggregator.OnlineUserModifierCombiner;
import com.voole.hobbit.storm.order.module.OnlineUser;
import com.voole.hobbit.storm.order.state.OnlineUserState;
import com.voole.hobbit.utils.Tuple;

public class OnlineUserStateQueryUpdater extends
		BaseQueryFunction<OnlineUserState, OnlineUser> {
	public static final Fields INPUT_FIELDS = OnlineUserModifierCombiner.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("global");

	@SuppressWarnings("unchecked")
	@Override
	public List<OnlineUser> batchRetrieve(OnlineUserState state,
			List<TridentTuple> args) {
		List<OnlineUser> list = new ArrayList<OnlineUser>();
		for (TridentTuple tuple : args) {
			list.add(state
					.update((Map<Tuple<String, Long>, OnlineUser>) tuple
							.get(0)));

		}
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, OnlineUser result,
			TridentCollector collector) {
		collector.emit(new Values(result));
	}

}