/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.ResourceInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface ResourceInfoCache extends HobbitCache {
	public Optional<ResourceInfo> getResourceInfo(String spid, String fid)
			throws CacheRefreshException, CacheQueryException;

	public static interface ResourceInfoFetch {
		Map<String, String> getSpidToMovieSpidMap();

		Map<Tuple<String, String>, ResourceInfo> getResourceMap();
	}
}
