/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.OrderSessionState.OrderSessionStateUpdater;
import com.voole.hobbit.storm.order.module.OrderOnlineUser;
import com.voole.hobbit.storm.order.module.OrderOnlineUserModifier;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OrderOnlineUserModifierCombinerAggregator implements
		CombinerAggregator<Map<Tuple<String, Long>, OrderOnlineUser>> {
	public static final Fields INPUT_FIELDS = OrderSessionStateUpdater.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("modifier_combiner");

	@Override
	public Map<Tuple<String, Long>, OrderOnlineUser> init(TridentTuple tuple) {
		OrderOnlineUserModifier modifier = (OrderOnlineUserModifier) tuple
				.get(0);
		Map<Tuple<String, Long>, OrderOnlineUser> map = new HashMap<Tuple<String, Long>, OrderOnlineUser>();
		map.put(modifier.getKey(), modifier);
		return map;
	}

	@Override
	public Map<Tuple<String, Long>, OrderOnlineUser> combine(
			Map<Tuple<String, Long>, OrderOnlineUser> val1,
			Map<Tuple<String, Long>, OrderOnlineUser> val2) {
		for (Entry<Tuple<String, Long>, OrderOnlineUser> entry : val2
				.entrySet()) {
			Tuple<String, Long> key = entry.getKey();
			OrderOnlineUser v = entry.getValue();
			if (val1.containsKey(key)) {
				val1.get(key).update(v);
			} else {
				val1.put(key, v);
			}
		}
		return val1;
	}

	@Override
	public Map<Tuple<String, Long>, OrderOnlineUser> zero() {
		return new HashMap<Tuple<String, Long>, OrderOnlineUser>();
	}

}
