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

import com.voole.hobbit.cachestate.cache.AreaInfoCache;
import com.voole.hobbit.cachestate.entity.AreaInfo;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitStateCommon;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class AreaInfoQueryFunction extends
		BaseQueryFunction<HobbitStateCommon<AreaInfoCache>, AreaInfo> {
	public static final Fields INPUT_FIELDS = new Fields("extra", "spid");
	public static final Fields OUTPUT_FIELDS = new Fields("areainfo");

	@Override
	public List<AreaInfo> batchRetrieve(HobbitStateCommon<AreaInfoCache> state,
			List<TridentTuple> args) {
		List<AreaInfo> list = new ArrayList<AreaInfo>();
		for (TridentTuple tuple : args) {
			PlayBgnExtra extra = (PlayBgnExtra) tuple.get(0);
			String spid = (String) tuple.get(1);
			list.add(state.getCache().getAreaInfo(extra.getHid(),
					extra.getOemid().toString(), spid, extra.getNatip()));
		}
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, AreaInfo result,
			TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
