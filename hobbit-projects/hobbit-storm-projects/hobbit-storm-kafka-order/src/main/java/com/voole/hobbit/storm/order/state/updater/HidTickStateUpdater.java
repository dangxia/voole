/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.updater;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.function.aggregator.SessionTickCombinerAggregator;
import com.voole.hobbit.storm.order.module.session.SessionTick;
import com.voole.hobbit.storm.order.state.HidTickState;
import com.voole.hobbit.storm.order.state.HidTickStateImpl.HidTickStateFactory;

public class HidTickStateUpdater implements StateUpdater<HidTickState> {
	public final static Fields INPUT_FIELDS = SessionTickCombinerAggregator.OUTPUT_FIELDS;
	public final static Fields OUTPUT_FIELDS = new Fields("modifier");
	public final static Fields PARTITION_FIELDS = new Fields("hid");

	public static TridentState partitionPersist(Stream stream) {
		return stream.partitionBy(PARTITION_FIELDS).partitionPersist(
				new HidTickStateFactory(), INPUT_FIELDS,
				new HidTickStateUpdater(), OUTPUT_FIELDS);
	}

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
		List<SessionTick> ticks = new ArrayList<SessionTick>();
		for (TridentTuple tuple : tuples) {
			ticks.add((SessionTick) tuple.get(0));
		}
		state.update(ticks, collector);
	}

}