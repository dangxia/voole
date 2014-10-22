package com.voole.dungbeetle.ad.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.stereotype.Repository;

import com.voole.dungbeetle.ad.dao.IPlayLogDao;
import com.voole.dungbeetle.ad.model.AdInfo;
import com.voole.dungbeetle.ad.model.AdPlayStat;
import com.voole.dungbeetle.ad.util.DataSourceType;

/**
 * 播放日志dao实现
 */
@Repository
public class PlayLogDaoImpl extends SqlSessionDaoSupport implements IPlayLogDao {

	
	/**
	 * 根据fid查询介质信息
	 * 
	 * @param fid
	 * @return
	 */
	public HashMap<String, Object> findAdVedioInfoByFid(String fid) {
		return this.getSqlSession().selectOne("playLog.findAdVedioInfoByFid", fid);
	}

	/**
	 * 
	 * 根据logid fid查询介质信息
	 * 
	 * @param map
	 * @return
	 */
	public HashMap<String, Object> findAdVedioInfoByLogid(Map<String, String> map){
		return this.getSqlSession().selectOne("playLog.findAdVedioInfoByLogid", map);
	}

	/* (non-Javadoc)
	 * @see com.voole.dao.IPlayLogDao#findAminfoByPlanId(java.util.Map)
	 */
	@Override
	public Map<String, Object> findAminfoByPlanId(Map<String, String> map)  {
		return this.getSqlSession().selectOne("playLog.findAminfoByPlanId", map);
	}

	@Override
	public List<Map<String, Object>> findAllAdPos() {
		return this.getSqlSession().selectList("playLog.findAllAdPos");
	}
	
	@Override
	public List<Map<String, Object>> findAllAreaInfos(){
		return this.getSqlSession().selectList("playLog.findAllAreaInfos");
	}

	/* (non-Javadoc)
	 * @see com.voole.dao.IPlayLogDao#findCapitalByProvinceid(java.lang.String)
	 */
	@Override
	public Map<String, Object> findCapitalByProvincecode(String provinceid) {
		return this.getSqlSession().selectOne("playLog.findCapitalByProvincecode", provinceid);
	}

	@Override
	public List<Map<String, Object>> findAllChannelProgramInfoAccessAd() {
		return this.getSqlSession().selectList("playLog.findAllChannelProgramInfoAccessAd");
	}

	@Override
	public List<String> findMonitorOemid() {
		return this.getSqlSession().selectList("playLog.findMonitorOemid");
	}

	@Override
	public List<String> findAmidByfid(String adfid) {
		return this.getSqlSession().selectList("playLog.findAmidByfid", adfid);
	}
	
	/**
	 * 缓存查询所有的广告信息 
	 * 以前通过amid(广告节目id)查询,现在查询所有的，然后用amid作为Map的key值
	 * @return
	 */
	@Override
	public List<Map<String, Object>> findAllAdinfo() {
		return  this.getSqlSession().selectList("playLog.findAllAdinfo");
	}

	@Override
	public List<Map<String, Object>> findAllChannelInfos() {
		return this.getSqlSession().selectList("playLog.findAllChannelInfos");
	}

}
