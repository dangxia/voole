/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.extra;

import storm.trident.Stream;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.function.transformer.TransformerFunction;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtraUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class PlayExtraFunction extends BaseFunction {
	public static final Fields INPUT_FIELDS = TransformerFunction.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("sessionId", "extra");

	public static Stream each(Stream stream) {
		return stream
				.each(INPUT_FIELDS, new PlayExtraFunction(), OUTPUT_FIELDS)
				.project(OUTPUT_FIELDS);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Object proto = tuple.get(0);
		PlayExtra extra = PlayExtraUtils.getExtra(proto);
		if (extra != null) {
			collector.emit(new Values(extra.getSessionId(), extra));
		}
	}

}
