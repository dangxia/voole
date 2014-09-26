/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
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

	public RangeMap<Long, AreaInfo> getVooleIpRanges() {
		String sql = " SELECT si.areaid, si.type AS nettype, si.`minip`, si.`maxip` FROM sys_ipzone si WHERE si.`maxip`>si.`minip` ";
		final RangeMap<Long, AreaInfo> result = TreeRangeMap.create();
		realtimeJt.query(sql, new RowMapper<Void>() {

			@Override
			public Void mapRow(ResultSet rs, int rowNum) throws SQLException {
				result.put(
						Range.closed(rs.getLong("minip"), rs.getLong("maxip")),
						new AreaInfo(rs.getInt("areaid"), rs.getInt("nettype")));

				return null;
			}
		});
		return result;
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
	public List<OemInfo> getOemInfos() {
		// TODO Auto-generated method stub
		return null;
	}

}
