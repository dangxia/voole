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

import com.voole.hobbit.cachestate.cache.OemInfoCache;
import com.voole.hobbit.cachestate.entity.OemInfo;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitStateCommon;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.utils.ProductUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class SpidQueryFunction extends
		BaseQueryFunction<HobbitStateCommon<OemInfoCache>, String> {
	public static final Fields INPUT_FIELDS = new Fields("extra");
	public static final Fields OUTPUT_FIELDS = new Fields("spid");

	@Override
	public List<String> batchRetrieve(HobbitStateCommon<OemInfoCache> state,
			List<TridentTuple> args) {
		List<String> list = new ArrayList<String>();
		for (TridentTuple tuple : args) {
			PlayBgnExtra extra = (PlayBgnExtra) tuple.get(0);
			list.add(getSpid(state, extra.getOemid()));
		}
		return list;
	}

	protected String getSpid(HobbitStateCommon<OemInfoCache> state, Long oemid) {
		OemInfo oemInfo = null;
		if (oemid != null) {
			oemInfo = state.getCache().getOemInfo(oemid.intValue());
		}
		if (oemInfo == null) {
			return ProductUtils.VOOLE_SPID.toString();
		} else {
			return oemInfo.getSpid();
		}
	}

	@Override
	public void execute(TridentTuple tuple, String result,
			TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
