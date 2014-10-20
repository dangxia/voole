/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.ParentSectionInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface ParentSectionInfoCache extends HobbitCache {
	public Optional<ParentSectionInfo> getParentSectionInfo(String sectionid)
			throws CacheRefreshException, CacheQueryException;

	public static interface ParentSectionInfoFetch {
		Map<String, ParentSectionInfo> getParentSectionMap();
	}
}
