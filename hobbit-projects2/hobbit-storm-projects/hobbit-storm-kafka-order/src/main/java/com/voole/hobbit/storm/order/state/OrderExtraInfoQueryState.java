/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.cachestate.cache.AreaInfoCache;
import com.voole.hobbit.cachestate.cache.AreaInfoCacheImpl;
import com.voole.hobbit.cachestate.cache.OemInfoCache;
import com.voole.hobbit.cachestate.cache.OemInfoCacheImpl;
import com.voole.hobbit.cachestate.cache.ResourceInfoCache;
import com.voole.hobbit.cachestate.cache.ResourceInfoCacheImpl;
import com.voole.hobbit.cachestate.db.CacheDao;
import com.voole.hobbit.cachestate.db.CacheDaoUtil;
import com.voole.hobbit.cachestate.entity.AreaInfo;
import com.voole.hobbit.cachestate.entity.OemInfo;
import com.voole.hobbit.cachestate.entity.ResourceInfo;
import com.voole.hobbit.cachestate.state.HobbitCompositeCacheState;
import com.voole.hobbit.storm.order.function.aggregator.PlayExtraCombinerAggregator;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.utils.ProductUtils;

/**
 * 
 * 额外信息的组合State
 * 
 * @author XuehuiHe
 * @date 2014年6月25日
 */
public class OrderExtraInfoQueryState extends HobbitCompositeCacheState {
	private final AreaInfoCache areaInfoCache;
	private final OemInfoCache oemInfoCache;
	private final ResourceInfoCache resourceInfoCache;

	public OrderExtraInfoQueryState(AreaInfoCache areaInfoCache,
			OemInfoCache oemInfoCache, ResourceInfoCache resourceInfoCache) {
		super(areaInfoCache, oemInfoCache, resourceInfoCache);
		this.areaInfoCache = areaInfoCache;
		this.oemInfoCache = oemInfoCache;
		this.resourceInfoCache = resourceInfoCache;
	}

	public AreaInfoCache getAreaInfoCache() {
		return areaInfoCache;
	}

	public OemInfoCache getOemInfoCache() {
		return oemInfoCache;
	}

	public ResourceInfoCache getResourceInfoCache() {
		return resourceInfoCache;
	}

	public static class OrderExtraInfoQueryStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			CacheDao dao = CacheDaoUtil.getCacheDao();
			return new OrderExtraInfoQueryState(new AreaInfoCacheImpl(dao),
					new OemInfoCacheImpl(dao), new ResourceInfoCacheImpl(dao));
		}
	}

	public static class OrderExtraInfoQuery extends
			BaseQueryFunction<OrderExtraInfoQueryState, PlayExtra> {

		public static final Fields INPUT_FIELDS = PlayExtraCombinerAggregator.OUTPUT_FIELDS;
		public static final Fields OUTPUT_FIELDS = new Fields("s", "extra2");

		public static Stream query(Stream stream, TridentState state) {
			return stream.stateQuery(state, INPUT_FIELDS,
					new OrderExtraInfoQuery(), OUTPUT_FIELDS).project(
					OUTPUT_FIELDS);
		}

		@Override
		public List<PlayExtra> batchRetrieve(OrderExtraInfoQueryState state,
				List<TridentTuple> args) {
			List<PlayExtra> list = new ArrayList<PlayExtra>();
			for (TridentTuple tuple : args) {
				PlayExtra extra = (PlayExtra) tuple.get(0);
				if (extra.getPlayType().isEmpty()) {
					list.add(null);
				} else if (!extra.getPlayType().isBgn()) {
					list.add(extra);
				} else {
					PlayBgnExtra _extra = (PlayBgnExtra) extra;
					String spid = getSpid(state, _extra.getOemid());
					AreaInfo areaInfo = getAreaInfo(state, _extra, spid);
					ResourceInfo resourceInfo = getResourceInfo(state, _extra,
							spid);
					_extra.setSpid(spid);
					if (areaInfo != null) {
						_extra.setAreaid(areaInfo.getAreaid());
						_extra.setNettype(areaInfo.getNettype());
					}
					if (resourceInfo != null) {
						_extra.setBitrate(resourceInfo.getBitrate());
					}
					_extra.calcVip();
					_extra.calcLow();
					list.add(_extra);
				}
			}
			return list;
		}

		protected String getSpid(OrderExtraInfoQueryState state, Long oemid) {
			OemInfo oemInfo = null;
			if (oemid != null) {
				oemInfo = state.getOemInfoCache().getOemInfo(oemid);
			}
			if (oemInfo == null) {
				return ProductUtils.VOOLE_SPID.toString();
			} else {
				return oemInfo.getSpid();
			}
		}

		protected AreaInfo getAreaInfo(OrderExtraInfoQueryState state,
				PlayBgnExtra extra, String spid) {
			return state.getAreaInfoCache().getAreaInfo(extra.getHid(),
					extra.getOemid().toString(), spid, extra.getNatip());
		}

		protected ResourceInfo getResourceInfo(OrderExtraInfoQueryState state,
				PlayBgnExtra extra, String spid) {
			return state.getResourceInfoCache().getResourceInfo(spid,
					extra.getFid());
		}

		@Override
		public void execute(TridentTuple tuple, PlayExtra result,
				TridentCollector collector) {
			if (result != null) {
				collector.emit(new Values(result.getSessionId(), result));
			}
		}

	}

}
