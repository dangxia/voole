/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.voole.hobbit.cache.AbstractRefreshState;
import com.voole.hobbit.cache.db.CacheDao;
import com.voole.hobbit.cache.db.CacheDaoUtil;
import com.voole.hobbit.cache.entity.OemInfo;

/**
 * @author XuehuiHe
 * @date 2014年6月11日
 */
public class OemInfoCacheState extends AbstractRefreshState {
	public static final String NAME = "oeminfo-cache-state";
	private static Logger logger = LoggerFactory
			.getLogger(OemInfoCacheState.class);

	private final CacheDao cacheDao;
	private Map<Integer, OemInfo> oemInfoMap;
	private final AtomicLong times;

	public OemInfoCacheState(CacheDao cacheDao) {
		super(NAME);
		this.cacheDao = cacheDao;
		times = new AtomicLong(0);
		doRefresh();
	}

	@Override
	public void beginCommit(Long txid) {

	}

	@Override
	public void commit(Long txid) {

	}

	public OemInfo getOemInfo(Integer oemid) {
		long t = times.incrementAndGet();
		if (t != 1) {
			logger.info(getName() + " getOemInfo start,times:" + t);
		}

		OemInfo info = null;
		try {
			info = oemInfoMap.get(oemid);
		} catch (Exception e) {
			logger.warn("OemInfoCacheState getOemInfo error", e);
		} finally {
		}
		t = times.decrementAndGet();
		if (t != 0) {
			logger.info("-----------------------" + getName()
					+ "getOemInfo end,times:" + t);
		}
		return info;
	}

	@Override
	protected void doRefresh() {
		logger.info(getName() + " do refresh start,times:"
				+ times.incrementAndGet());
		try {
			List<OemInfo> list = cacheDao.getOemInfos();
			oemInfoMap = new HashMap<Integer, OemInfo>();
			for (OemInfo oemInfo : list) {
				oemInfoMap.put(oemInfo.getOemid(), oemInfo);
			}
		} catch (Exception e) {
			logger.warn("OemInfoCacheState doRefresh error", e);
		}
		logger.info("-----------------------" + getName()
				+ " do refresh end,times:" + times.decrementAndGet());
	}

	public static class OemInfoCacheStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			OemInfoCacheState state = new OemInfoCacheState(
					CacheDaoUtil.getCacheDao());
			return state;
		}
	}

}
