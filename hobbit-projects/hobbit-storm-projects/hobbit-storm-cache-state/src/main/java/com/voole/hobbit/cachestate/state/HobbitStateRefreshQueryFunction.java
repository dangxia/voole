/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class HobbitStateRefreshQueryFunction extends
		BaseQueryFunction<HobbitState, Void> {
	private final boolean isDelay;

	public HobbitStateRefreshQueryFunction(boolean isDelay) {
		this.isDelay = isDelay;
	}

	public HobbitStateRefreshQueryFunction() {
		this(true);
	}

	@Override
	public List<Void> batchRetrieve(HobbitState state,
			List<TridentTuple> args) {
		List<Void> list = new ArrayList<Void>();
		for (TridentTuple tuple : args) {
			@SuppressWarnings("unchecked")
			List<String> cmds = (List<String>) tuple.get(0);
			if (isDelay) {
				state.refreshDelay(cmds);
			} else {
				state.refreshImmediately(cmds);
			}
			list.add(null);
		}
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, Void result,
			TridentCollector collector) {
	}

}