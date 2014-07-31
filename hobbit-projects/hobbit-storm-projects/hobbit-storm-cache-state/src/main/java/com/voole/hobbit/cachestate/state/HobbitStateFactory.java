/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.voole.hobbit.cache.AreaInfoCacheImpl;
import com.voole.hobbit.cache.HobbitCache;
import com.voole.hobbit.cache.OemInfoCacheImpl;
import com.voole.hobbit.cache.ResourceInfoCacheImpl;
import com.voole.hobbit.cache.db.CacheDao;
import com.voole.hobbit.cache.db.CacheDaoUtil;

public abstract class HobbitStateFactory implements StateFactory {

	@Override
	public State makeState(@SuppressWarnings("rawtypes") Map conf,
			IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return makeState(CacheDaoUtil.getCacheDao());
	}

	public State makeState(CacheDao cacheDao) {
		return new HobbitSingleCacheState<HobbitCache>(getCache(cacheDao));
	}

	protected abstract HobbitCache getCache(CacheDao cacheDao);

	public static class HobbitAreaInfoStateFactory extends HobbitStateFactory {
		@Override
		protected HobbitCache getCache(CacheDao cacheDao) {
			return new AreaInfoCacheImpl(cacheDao);
		}
	}

	public static class HobbitOemInfoStateFactory extends HobbitStateFactory {
		@Override
		protected HobbitCache getCache(CacheDao cacheDao) {
			return new OemInfoCacheImpl(cacheDao);
		}
	}

	public static class HobbitResourceInfoStateFactory extends
			HobbitStateFactory {
		@Override
		protected HobbitCache getCache(CacheDao cacheDao) {
			return new ResourceInfoCacheImpl(cacheDao);
		}
	}
}