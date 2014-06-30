/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.query;

import java.util.ArrayList;
import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.state.OnlineUserState;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class OnlineUserQuery extends BaseQueryFunction<OnlineUserState, String> {

	public static Stream query(Stream stream, TridentState state) {
		return stream.stateQuery(state, new OnlineUserQuery(), OUTPUT_FIELDS)
				.project(OUTPUT_FIELDS);
	}

	public static Fields OUTPUT_FIELDS = new Fields("result");

	@Override
	public List<String> batchRetrieve(OnlineUserState state,
			List<TridentTuple> args) {
		List<String> list = new ArrayList<String>();
		list.add(state.query());
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, String result,
			TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
