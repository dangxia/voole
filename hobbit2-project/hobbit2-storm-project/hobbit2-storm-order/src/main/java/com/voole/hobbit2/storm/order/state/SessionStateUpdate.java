/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;

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
		List<SpecificRecordBase> list = new ArrayList<SpecificRecordBase>();
		for (TridentTuple tridentTuple : tuples) {
			list.add((SpecificRecordBase) tridentTuple.get(0));
		}
		state.update(list);

	}

}
