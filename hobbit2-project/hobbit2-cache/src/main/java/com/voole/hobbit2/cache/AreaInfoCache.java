/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.RangeMap;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface AreaInfoCache extends HobbitCache {
	public Optional<AreaInfo> getAreaInfoNormal(Long ip)
			throws CacheQueryException, CacheRefreshException;

	// public Optional<AreaInfo> getAreaInfoFromBoxStore(String oemid, String
	// hid)
	// throws CacheQueryException, CacheRefreshException;

	public Optional<AreaInfo> getAreaInfoFromSp(String spid, Long ip)
			throws CacheQueryException, CacheRefreshException;

	public Optional<AreaInfo> getAreaInfo(String spid, Long ip)
			throws CacheQueryException, CacheRefreshException;

	public static interface AreaInfosFetch {
		// public List<BoxStoreAreaInfo> getLiveBoxStoreAreaInfos();

		public Map<String, RangeMap<Long, AreaInfo>> getSpIpRanges();

		public RangeMap<Long, AreaInfo> getVooleIpRanges();
	}
}
