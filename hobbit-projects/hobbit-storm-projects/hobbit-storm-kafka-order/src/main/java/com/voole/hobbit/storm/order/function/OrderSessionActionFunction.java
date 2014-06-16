/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.OrderSessionState.OrderAction;
import com.voole.hobbit.storm.order.OrderSessionState.OrderSessionStateUpdater;
import com.voole.hobbit.storm.order.module.OrderBgnSessionInfo;
import com.voole.hobbit.storm.order.module.OrderOnlineUserModifier;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OrderSessionActionFunction extends BaseFunction {
	public static final Fields INPUT_FIELDS = OrderSessionStateUpdater.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("change");

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		OrderAction action = (OrderAction) tuple.get(0);
		OrderBgnSessionInfo sessionInfo = (OrderBgnSessionInfo) tuple
				.get(1);

		OrderOnlineUserModifier modifier = new OrderOnlineUserModifier(
				sessionInfo.getSpid(), sessionInfo.getExtra().getOemid());
		modifier.setUserNum(action == OrderAction.INCREASE ? 1 : -1);
		if (sessionInfo.isVip()) {
			modifier.setVipNum(action == OrderAction.INCREASE ? 1 : -1);
		}
		collector.emit(new Values(modifier));

	}

}
