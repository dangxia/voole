/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.RangeMap;
import com.voole.hobbit2.cache.AreaInfoCache.AreaInfosFetch;
import com.voole.hobbit2.cache.OemInfoCache.OemInfoFetch;
import com.voole.hobbit2.cache.ResourceInfoCache.ResourceInfoFetch;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.BoxStoreAreaInfo;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.ResourceInfo;
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TestBean implements OemInfoFetch, AreaInfosFetch,
		ResourceInfoFetch {
	private JdbcTemplate realtimeJt;

	public JdbcTemplate getRealtimeJt() {
		return realtimeJt;
	}

	public void setRealtimeJt(JdbcTemplate realtimeJt) {
		this.realtimeJt = realtimeJt;
	}

	@Override
	public Map<String, String> getSpidToMovieSpidMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Tuple<String, String>, ResourceInfo> getResourceMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BoxStoreAreaInfo> getLiveBoxStoreAreaInfos() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, RangeMap<Long, AreaInfo>> getSpIpRanges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RangeMap<Long, AreaInfo> getVooleIpRanges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<OemInfo> getOemInfos() {
		// TODO Auto-generated method stub
		return null;
	}

}
