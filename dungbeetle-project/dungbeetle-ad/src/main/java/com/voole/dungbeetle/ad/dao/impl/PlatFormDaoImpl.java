package com.voole.dungbeetle.ad.dao.impl;

import java.util.List;
import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.stereotype.Repository;

import com.voole.dungbeetle.ad.dao.IPlatFormDao;
import com.voole.dungbeetle.ad.util.DataSourceType;
@Repository
public class PlatFormDaoImpl extends SqlSessionDaoSupport implements IPlatFormDao {

	@Override
	@DataSourceType(type="platform")
	public List<Map<String,Object>> findOemids(Map<String,String> params) {
		return getSqlSession().selectList("platForm.findOemidInfo", params);
	}
	
	/**
	 * 通过sectionid(栏目id)，获取该栏目信息及该栏目所属的频道信息
	 */
	@Override
	@DataSourceType(type="platform")
	public List<Map<String, Object>> findAllChannelProgramInfoAccessPlatform(){
		return getSqlSession().selectList("platForm.findAllPrograms");
	}
	
	@Override
	@DataSourceType(type="platform")
	public List<Map<String,Object>> findAllMovieTypes() {
		return getSqlSession().selectList("platForm.findAllMovieTypes");
	}
}
