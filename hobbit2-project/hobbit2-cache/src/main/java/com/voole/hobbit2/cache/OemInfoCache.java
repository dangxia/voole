/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.List;

import com.voole.hobbit2.cache.entity.OemInfo;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface OemInfoCache extends HobbitCache {
	public OemInfo getOemInfo(Long oemid);

	public static interface OemInfoFetch {
		public List<OemInfo> getOemInfos();
	}
}
