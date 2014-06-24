/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.extra;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.storm.order.function.transformer.TransformerFunction;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class PlayBgnExtraFunction extends BaseFunction {
	public static final Fields INPUT_FIELDS = TransformerFunction.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("extra");

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		PlayBgnExtra extra = new PlayBgnExtra();
		Object proto = tuple.get(0);
		if (proto instanceof OrderPlayBgnReqV2) {
			extra.fillWith((OrderPlayBgnReqV2) proto);
		} else if (proto instanceof OrderPlayBgnReqV3) {
			extra.fillWith((OrderPlayBgnReqV3) proto);
		}
		collector.emit(new Values(extra));
	}

}
