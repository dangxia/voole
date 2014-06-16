/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.cachestate.entity.AreaInfo;
import com.voole.hobbit.cachestate.entity.ResourceInfo;
import com.voole.hobbit.storm.order.module.OrderBgnSessionInfo;
import com.voole.hobbit.storm.order.module.extra.OrderPlayBgnExtra;

public class AssemblyOrderSession extends BaseFunction {
	public final static Fields INPUT_FIELDS = new Fields("extra", "spid",
			"areainfo", "resourceinfo");
	public final static Fields OUTPUT_FIELDS = new Fields("sessionId",
			"session");

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		OrderPlayBgnExtra extra = (OrderPlayBgnExtra) tuple.get(0);
		String spid = tuple.getString(1);
		AreaInfo areaInfo = (AreaInfo) tuple.get(2);
		ResourceInfo resourceInfo = (ResourceInfo) tuple.get(3);
		collector.emit(new Values(extra.getSessionId(),
				OrderBgnSessionInfo.create(extra, spid, areaInfo,
						resourceInfo)));
	}

}