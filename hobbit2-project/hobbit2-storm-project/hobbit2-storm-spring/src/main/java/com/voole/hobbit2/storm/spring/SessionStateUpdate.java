/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateUpdate implements StateUpdater<SessionState> {

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void updateState(SessionState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		List<String> list = new ArrayList<String>();
		for (TridentTuple tridentTuple : tuples) {
			list.add((String) tridentTuple.get(0));
		}
		state.update(list);

	}

}
