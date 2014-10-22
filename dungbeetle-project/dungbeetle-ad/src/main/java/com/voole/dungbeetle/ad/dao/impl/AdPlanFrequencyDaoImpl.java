package com.voole.dungbeetle.ad.dao.impl;

import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.stereotype.Repository;

import com.voole.dungbeetle.ad.dao.IAdPlanFrequencyDao;
import com.voole.dungbeetle.ad.util.DataSourceType;

@Repository
public class AdPlanFrequencyDaoImpl extends SqlSessionDaoSupport implements IAdPlanFrequencyDao{

	@Override
	@DataSourceType(type="ad")
	public int updateAdPuttimes(Map<String, Object> map) {
		this.getSqlSession().update("adPlanFrequency.updateAdPuttimes", map);
		return 0;
	}
}
