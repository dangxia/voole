/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

/**
 * @author XuehuiHe
 * @date 2014年6月9日
 */
public abstract class AbstractRefreshState implements State {
	private final String name;

	public AbstractRefreshState(String name) {
		this.name = name;
	}

	public void refresh(List<String> cmds) {
		if (isShouldRefresh(cmds)) {
			doRefresh();
		}
	}

	protected boolean isShouldRefresh(List<String> cmds) {
		if (cmds.size() == 0) {
			return false;
		}
		String stateName = getName();
		for (String cmd : cmds) {
			if (cmd != null && (stateName.equals(cmd) || "all".equals(cmd))) {
				return true;
			}
		}
		return false;
	}

	protected abstract void doRefresh();

	protected String getName() {
		return this.name;
	}

	public static class StateRefreshQueryFunction extends
			BaseQueryFunction<AbstractRefreshState, Void> {
		public static final Fields INPUT_FIELDS = RefreshCmdSender.OUTPUT_FIELDS;
		public static final Fields OUTPUT_FIELDS = new Fields();

		@Override
		public List<Void> batchRetrieve(AbstractRefreshState state,
				List<TridentTuple> args) {
			List<Void> list = new ArrayList<Void>();
			for (TridentTuple tuple : args) {
				@SuppressWarnings("unchecked")
				List<String> cmds = (List<String>) tuple.get(0);
				state.refresh(cmds);
				list.add(null);
			}
			return list;
		}

		@Override
		public void execute(TridentTuple tuple, Void result,
				TridentCollector collector) {

		}

	}
}
