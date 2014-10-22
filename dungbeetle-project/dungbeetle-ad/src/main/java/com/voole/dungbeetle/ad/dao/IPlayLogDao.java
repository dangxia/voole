package com.voole.dungbeetle.ad.dao;

import java.util.List;
import java.util.Map;

import com.voole.dungbeetle.ad.model.AdInfo;
import com.voole.dungbeetle.ad.model.AdPlayStat;

/**
 * 播放日志dao
 */
public interface IPlayLogDao {
	
	/**
	 * 
	 *查找广告节目信息
	 * @param map
	 * @return
	 */
	public Map<String, Object> findAminfoByPlanId(Map<String, String> map);
	
	/**
	 * 以前是根据排期查找广告位位置
	 * 现在全部查询出来缓存
	 * @param planid
	 * @return
	 */
	public List<Map<String, Object>> findAllAdPos();
	
	/**
	 * 根据区域编码查询省份和城市信息
	 * @param areacode
	 * @return
	 */
	public List<Map<String, Object>> findAllAreaInfos();
	
	
	/**
	 * 根据省份id查询省会信息
	 * @param provinceid
	 * @return
	 */
	public Map<String, Object> findCapitalByProvincecode(String provinceid);
	
	/**
	 * 根据播控频道名、栏目名查统计的频道、栏目信息
	 * @param categorymap
	 * @return
	 */
	public List<Map<String, Object>> findAllChannelProgramInfoAccessAd();
	
	/**
	 * 监控oemid
	 * @return
	 */
	public List<String> findMonitorOemid();
	
	/**
	 * 根据广告fid查询广告节目id
	 * @param adfid
	 * @return
	 */
	public List<String> findAmidByfid(String adfid);
	
	/**
	 * 查询所有广告信息
	 * 以前是根据广告节目id
	 * @param amid
	 * @return
	 */
	public List<Map<String, Object>> findAllAdinfo();
	
	/**
	 * 根据节目类型（mtype）查询频道信息
	 * @param mtype
	 * @return
	 */
	public List<Map<String, Object>> findAllChannelInfos();
}
