/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import java.util.Map;

import com.voole.hobbit.cache.entity.ResourceInfo;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface ResourceInfoCache extends HobbitCache {
	public ResourceInfo getResourceInfo(String spid, String fid);

	public static interface ResourceInfoFetch {
		Map<String, String> getSpidToMovieSpidMap();

		Map<String, ResourceInfo> getResourceMap();
	}
}
