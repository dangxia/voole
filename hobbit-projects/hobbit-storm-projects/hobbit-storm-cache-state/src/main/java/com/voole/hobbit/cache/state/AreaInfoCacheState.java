/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.voole.hobbit.cache.AbstractRefreshState;
import com.voole.hobbit.cache.db.CacheDao;
import com.voole.hobbit.cache.db.CacheDaoUtil;
import com.voole.hobbit.cache.entity.AreaInfo;
import com.voole.hobbit.cache.entity.BoxStoreAreaInfo;
import com.voole.hobbit.cache.entity.IpRange;
import com.voole.hobbit.config.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月11日
 */
public class AreaInfoCacheState extends AbstractRefreshState {

	public static final String NAME = "areainfo-cache-state";

	private static Logger logger = LoggerFactory
			.getLogger(AreaInfoCacheState.class);

	private final CacheDao cacheDao;

	private Map<Tuple<String, String>, AreaInfo> boxStoreMap;
	private Map<String, TreeMap<Long, AreaInfo>> spIpRangeMap;
	private TreeMap<Long, AreaInfo> vooleIpRangeMap;

	public AreaInfoCacheState(CacheDao cacheDao) {
		super(NAME);
		this.cacheDao = cacheDao;
		doRefresh();
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	public AreaInfo getAreaInfo(String hid, String oemid, String spid, String ip) {
		AreaInfo rs = null;
		try {
			if (hid != null) {
				hid = hid.toUpperCase();
			}
			Tuple<String, String> key1 = new Tuple<String, String>(oemid, hid);
			if (boxStoreMap.containsKey(key1)) {
				rs = boxStoreMap.get(key1);
			}
			if (rs == null && spIpRangeMap.containsKey(spid)) {
				TreeMap<Long, AreaInfo> ipRang = spIpRangeMap.get(spid);
				Entry<Long, AreaInfo> entry = ipRang.floorEntry(ipToLong(ip));
				if (entry != null) {
					rs = entry.getValue();
				}
			}
			if (rs == null) {
				Entry<Long, AreaInfo> entry = vooleIpRangeMap
						.floorEntry(ipToLong(ip));
				if (entry != null) {
					rs = entry.getValue();
				}
			}

		} catch (Exception e) {
			logger.warn("AreaInfoCacheState getAreaInfo error", e);
		}
		return rs;
	}

	private Long ipToLong(String ip) {
		String[] ips = ip.split("[.]");
		long num = 16777216L * Long.parseLong(ips[0]) + 65536L
				* Long.parseLong(ips[1]) + 256 * Long.parseLong(ips[2])
				+ Long.parseLong(ips[3]);
		return num;
	}

	@Override
	protected void doRefresh() {
		logger.info(getName() + " do refresh start");
		try {
			boxStoreMap = getBoxStoreMapFromDb();
			spIpRangeMap = getSpIpRangeMapFromDb();
			vooleIpRangeMap = getVooleIpRangeMapFromDb();
		} catch (Exception e) {
			logger.warn("AreaInfoCacheState doRefresh error", e);
		}
		logger.info(getName() + " do refresh end");

	}

	protected TreeMap<Long, AreaInfo> getVooleIpRangeMapFromDb() {
		List<IpRange> ipRanges = cacheDao.getVooleIpRanges();
		return analyzeIpRange(ipRanges);
	}

	protected Map<Tuple<String, String>, AreaInfo> getBoxStoreMapFromDb() {
		Map<Tuple<String, String>, AreaInfo> boxStoreMap = new HashMap<Tuple<String, String>, AreaInfo>();
		List<BoxStoreAreaInfo> areaInfo1 = cacheDao.getLiveBoxStoreAreaInfos();
		for (BoxStoreAreaInfo areaInfo : areaInfo1) {
			String mac = areaInfo.getMac();
			if (mac != null) {
				mac = mac.toUpperCase();
			}
			boxStoreMap.put(
					new Tuple<String, String>(areaInfo.getOemid(), mac),
					new AreaInfo(areaInfo.getAreaid(), areaInfo.getNettype()));
		}
		return boxStoreMap;
	}

	protected Map<String, TreeMap<Long, AreaInfo>> getSpIpRangeMapFromDb() {
		Map<String, TreeMap<Long, AreaInfo>> spidRangMap = new HashMap<String, TreeMap<Long, AreaInfo>>();
		Map<String, List<IpRange>> spidToIpRanges = cacheDao.getSpIpRanges();
		for (Entry<String, List<IpRange>> entry : spidToIpRanges.entrySet()) {
			spidRangMap.put(entry.getKey(), analyzeIpRange(entry.getValue()));
		}
		return spidRangMap;
	}

	private TreeMap<Long, AreaInfo> analyzeIpRange(List<IpRange> ipRanges) {
		TreeMap<Long, Tuple<IpRange, Boolean>> map = new TreeMap<Long, Tuple<IpRange, Boolean>>();
		for (IpRange liveIpRange : ipRanges) {
			map.put(liveIpRange.getMinip(), new Tuple<IpRange, Boolean>(
					liveIpRange, true));
			map.put(liveIpRange.getMaxip(), new Tuple<IpRange, Boolean>(
					liveIpRange, false));
		}
		TreeMap<Long, AreaInfo> map2 = new TreeMap<Long, AreaInfo>();
		List<IpRange> stack = new ArrayList<IpRange>();
		map2.put(0l, null);
		for (Long ip : map.keySet()) {
			Tuple<IpRange, Boolean> t = map.get(ip);
			IpRange l = t.getA();
			if (t.getB()) {// ip start
				stack.add(l);
				map2.put(ip, new AreaInfo(l.getAreaid(), l.getNettype()));
			} else {// ip end
				if (stack.isEmpty()) {
					map2.put(ip, null);
				} else {
					stack.remove(l);
					if (stack.isEmpty()) {
						map2.put(ip + 1l, null);
					} else {
						IpRange p = stack.get(stack.size() - 1);
						map2.put(ip,
								new AreaInfo(p.getAreaid(), p.getNettype()));
					}
				}
			}
		}
		return map2;
	}

	public CacheDao getCacheDao() {
		return cacheDao;
	}

	public static class AreaInfoCacheStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			AreaInfoCacheState state = new AreaInfoCacheState(
					CacheDaoUtil.getCacheDao());
			return state;
		}
	}

}
