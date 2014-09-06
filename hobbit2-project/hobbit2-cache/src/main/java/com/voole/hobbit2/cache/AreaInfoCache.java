/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.List;
import java.util.Map;

import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.BoxStoreAreaInfo;
import com.voole.hobbit2.cache.entity.IpRange;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface AreaInfoCache extends HobbitCache {
	public AreaInfo getAreaInfoNormal(long ip);

	public AreaInfo getAreaInfoFromBoxStore(String oemid, String hid);

	public AreaInfo getAreaInfoFromSp(String spid, long ip);

	public AreaInfo getAreaInfo(String hid, String oemid, String spid, long ip);

	public static interface AreaInfosFetch {
		public List<BoxStoreAreaInfo> getLiveBoxStoreAreaInfos();

		public Map<String, List<IpRange>> getSpIpRanges();

		public List<IpRange> getVooleIpRanges();
	}
}
