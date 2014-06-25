/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.aggregator;

import storm.trident.Stream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.module.session.SessionTick;

/**
 * @author XuehuiHe
 * @date 2014年6月25日
 */
public class SessionTickCombinerAggregator implements
		CombinerAggregator<SessionTick> {
	public static final Fields INPUT_FIELDS = new Fields("tick");
	public static final Fields OUTPUT_FIELDS = new Fields("group_tick");
	public static final Fields GROUP_FIELDS = new Fields("hid");

	public static Stream group(Stream stream) {
		return stream.groupBy(GROUP_FIELDS).aggregate(INPUT_FIELDS,
				new SessionTickCombinerAggregator(), OUTPUT_FIELDS);
	}

	@Override
	public SessionTick init(TridentTuple tuple) {
		return (SessionTick) tuple.get(0);
	}

	@Override
	public SessionTick combine(SessionTick val1, SessionTick val2) {
		if (val1 == null) {
			return val2;
		}
		if (val2 == null) {
			return val1;
		}
		if (val1.getLastStamp() > val2.getLastStamp()) {
			return val1;
		}
		return val2;
	}

	@Override
	public SessionTick zero() {
		return null;
	}

}
