/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.updater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.module.session.SessionTick;
import com.voole.hobbit.storm.order.state.HidTickState;

public class HidTickStateUpdater implements StateUpdater<HidTickState> {
	public final static Fields INPUT_FIELDS = SessionStateUpdater.OUTPUT_FIELDS;
	public final static Fields OUTPUT_FIELDS = new Fields("modifier");

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void updateState(HidTickState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		// hid to tick
		Map<String, SessionTick> hidToTick = new HashMap<String, SessionTick>();
		for (TridentTuple tuple : tuples) {
			String hid = tuple.getString(0);
			SessionTick tick = (SessionTick) tuple.get(1);
			if (hidToTick.containsKey(hid)) {
				SessionTick old = hidToTick.get(hid);
				if (old.getLastStamp() < tick.getLastStamp()) {
					hidToTick.put(hid, tick);
				}
			} else {
				hidToTick.put(hid, tick);
			}
		}
		state.update(hidToTick, collector);
	}

}