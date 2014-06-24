/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.query;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.cachestate.cache.ResourceInfoCache;
import com.voole.hobbit.cachestate.entity.ResourceInfo;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitStateCommon;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ResourceQueryFunction extends
		BaseQueryFunction<HobbitStateCommon<ResourceInfoCache>, ResourceInfo> {
	public static final Fields INPUT_FIELDS = new Fields("extra", "spid");
	public static final Fields OUTPUT_FIELDS = new Fields("resourceinfo");

	@Override
	public List<ResourceInfo> batchRetrieve(
			HobbitStateCommon<ResourceInfoCache> state, List<TridentTuple> args) {
		List<ResourceInfo> list = new ArrayList<ResourceInfo>();
		for (TridentTuple tuple : args) {
			PlayBgnExtra extra = (PlayBgnExtra) tuple.get(0);
			String spid = (String) tuple.get(1);
			list.add(state.getCache().getResourceInfo(spid, extra.getFid()));
		}
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, ResourceInfo result,
			TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
