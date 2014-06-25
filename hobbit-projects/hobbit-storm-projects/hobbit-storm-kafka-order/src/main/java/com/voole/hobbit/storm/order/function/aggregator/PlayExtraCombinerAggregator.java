/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.aggregator;

import storm.trident.Stream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.module.extra.PlayAliveExtra;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayEndExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;

public class PlayExtraCombinerAggregator implements
		CombinerAggregator<PlayExtra> {
	public static final Fields INPUT_FIELDS = new Fields("extra");
	public static final Fields OUTPUT_FIELDS = new Fields("group_extra");
	public static final Fields GROUP_FIELDS = new Fields("sessionId");

	public static Stream group(Stream stream) {
		return stream
				.groupBy(GROUP_FIELDS)
				.aggregate(INPUT_FIELDS, new PlayExtraCombinerAggregator(),
						OUTPUT_FIELDS).project(OUTPUT_FIELDS);
	}

	@Override
	public PlayExtra init(TridentTuple tuple) {
		return (PlayExtra) tuple.get(0);
	}

	@Override
	public PlayExtra combine(PlayExtra val1, PlayExtra val2) {
		if (val1 == null) {
			return val2;
		}
		if (val2 == null) {
			return val1;
		}
		if (val1.getPlayType().isBgn()) {
			if (val2.getPlayType().isBgn()) {
				return _combine((PlayBgnExtra) val1, (PlayBgnExtra) val2);
			} else if (val2.getPlayType().isAlive()) {
				return _combine((PlayBgnExtra) val1, (PlayAliveExtra) val2);
			} else {
				return _combine((PlayBgnExtra) val1, (PlayEndExtra) val2);
			}
		} else if (val1.getPlayType().isAlive()) {
			if (val2.getPlayType().isBgn()) {
				return _combine((PlayBgnExtra) val2, (PlayAliveExtra) val1);
			} else if (val2.getPlayType().isAlive()) {
				return _combine((PlayAliveExtra) val1, (PlayAliveExtra) val2);
			} else {
				return _combine((PlayAliveExtra) val1, (PlayEndExtra) val2);
			}
		} else {
			if (val2.getPlayType().isBgn()) {
				return _combine((PlayBgnExtra) val2, (PlayEndExtra) val1);
			} else if (val2.getPlayType().isAlive()) {
				return _combine((PlayAliveExtra) val2, (PlayEndExtra) val1);
			} else {
				return _combine((PlayEndExtra) val1, (PlayEndExtra) val2);
			}
		}
	}

	private PlayExtra _same(PlayExtra val1, PlayExtra val2) {
		return val1.lastStamp() > val2.lastStamp() ? val1 : val2;
	}

	private PlayExtra _combine(PlayEndExtra val1, PlayEndExtra val2) {
		return _same(val1, val2);
	}

	private PlayExtra _combine(PlayAliveExtra val1, PlayAliveExtra val2) {
		return _same(val1, val2);
	}

	private PlayExtra _combine(PlayBgnExtra val1, PlayBgnExtra val2) {
		return _same(val1, val2);
	}

	private PlayExtra _combine(PlayAliveExtra val1, PlayEndExtra val2) {
		return _same(val1, val2);
	}

	private PlayExtra _combine(PlayBgnExtra val1, PlayEndExtra val2) {
		return _same(val1, val2);
	}

	private PlayExtra _combine(PlayBgnExtra val1, PlayAliveExtra val2) {
		if (val1.lastStamp() >= val2.lastStamp()) {
			return val1;
		}
		val1.update(val2);
		return val1;
	}

	@Override
	public PlayExtra zero() {
		return null;
	}

}