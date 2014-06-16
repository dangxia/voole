/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;

import com.voole.hobbit.cachestate.cache.AreaInfoCacheImpl;
import com.voole.hobbit.cachestate.cache.HobbitCache;
import com.voole.hobbit.cachestate.cache.OemInfoCacheImpl;
import com.voole.hobbit.cachestate.cache.ResourceInfoCacheImpl;
import com.voole.hobbit.cachestate.db.CacheDao;
import com.voole.hobbit.cachestate.db.CacheDaoUtil;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface HobbitState extends State {
	public void refreshDelay(List<String> cmds);

	public void refreshImmediately(List<String> cmds);

	public static class HobbitStateCommon<T extends HobbitCache> implements
			HobbitState {
		private static final Logger logger = LoggerFactory
				.getLogger(HobbitStateCommon.class);
		private final T cache;

		public HobbitStateCommon(T cache) {
			this.cache = cache;
		}

		public String getName() {
			return getCache().getName();
		}

		protected Logger getLogger() {
			return logger;
		}

		public T getCache() {
			return this.cache;
		}

		@Override
		public void beginCommit(Long txid) {
		}

		@Override
		public void commit(Long txid) {
		}

		@Override
		public void refreshDelay(List<String> cmds) {
			if (isShouldRefresh(cmds)) {
				getCache().refreshDelay();
			}
		}

		@Override
		public void refreshImmediately(List<String> cmds) {
			if (isShouldRefresh(cmds)) {
				getCache().refreshImmediately();
			}
		}

		protected boolean isShouldRefresh(List<String> cmds) {
			if (cmds.size() == 0) {
				return false;
			}
			String stateName = getName();
			for (String cmd : cmds) {
				if (cmd != null && (stateName.equals(cmd) || "all".equals(cmd))) {
					getLogger().info(
							stateName + " receive cmd:" + cmd
									+ " which permit for refresh");
					return true;
				}
			}
			return false;
		}
	}

	public abstract static class HobbitStateFactory implements StateFactory {

		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return makeState(CacheDaoUtil.getCacheDao());
		}

		public State makeState(CacheDao cacheDao) {
			return new HobbitStateCommon<HobbitCache>(getCache(cacheDao));
		}

		protected abstract HobbitCache getCache(CacheDao cacheDao);
	}

	public static class HobbitStateRefreshQueryFunction extends
			BaseQueryFunction<HobbitState, Void> {
		private final boolean isDelay;

		public HobbitStateRefreshQueryFunction(boolean isDelay) {
			this.isDelay = isDelay;
		}

		public HobbitStateRefreshQueryFunction() {
			this(true);
		}

		@Override
		public List<Void> batchRetrieve(HobbitState state,
				List<TridentTuple> args) {
			List<Void> list = new ArrayList<Void>();
			for (TridentTuple tuple : args) {
				@SuppressWarnings("unchecked")
				List<String> cmds = (List<String>) tuple.get(0);
				if (isDelay) {
					state.refreshDelay(cmds);
				} else {
					state.refreshImmediately(cmds);
				}
				list.add(null);
			}
			return list;
		}

		@Override
		public void execute(TridentTuple tuple, Void result,
				TridentCollector collector) {
		}

	}

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
