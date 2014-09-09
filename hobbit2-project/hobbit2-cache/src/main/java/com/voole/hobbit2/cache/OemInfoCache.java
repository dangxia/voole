/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.List;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface OemInfoCache extends HobbitCache {
	public Optional<OemInfo> getOemInfo(Long oemid)
			throws CacheRefreshException, CacheQueryException;

	public static interface OemInfoFetch {
		public List<OemInfo> getOemInfos();
	}
}
