/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit.cache.entity.AreaInfo;
import com.voole.hobbit.cache.entity.BoxStoreAreaInfo;
import com.voole.hobbit.cache.entity.IpRange;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class AreaInfoCacheImpl extends AbstractHobbitCache implements
		AreaInfoCache {
	private static final Logger logger = LoggerFactory
			.getLogger(AreaInfoCacheImpl.class);
	private final AreaInfosFetch fetch;

	// OEMID HID=>AREAINFO
	private Map<Tuple<String, String>, AreaInfo> boxStoreMap;
	// SPID=>(IP=>AREEINFO)
	private Map<String, TreeMap<Long, AreaInfo>> spIpRangeMap;
	// IP=>AREAINFO
	private TreeMap<Long, AreaInfo> vooleIpRangeMap;

	private Map<Tuple<String, String>, AreaInfo> boxStoreMapSwap;
	private Map<String, TreeMap<Long, AreaInfo>> spIpRangeMapSwap;
	private TreeMap<Long, AreaInfo> vooleIpRangeMapSwap;

	public AreaInfoCacheImpl(AreaInfosFetch fetch) {
		super("areainfo-cache-impl");
		this.fetch = fetch;
		refreshImmediately();
	}

	@Override
	public AreaInfo getAreaInfoNormal(long ip) {
		swop();
		AreaInfo rs = null;
		Entry<Long, AreaInfo> entry = vooleIpRangeMap.floorEntry(ip);
		if (entry != null) {
			rs = entry.getValue();
		}
		return rs;
	}

	@Override
	public AreaInfo getAreaInfoFromBoxStore(String oemid, String hid) {
		swop();
		AreaInfo rs = null;
		if (hid != null) {
			hid = hid.toUpperCase();
		}
		Tuple<String, String> key1 = new Tuple<String, String>(oemid, hid);
		if (boxStoreMap.containsKey(key1)) {
			rs = boxStoreMap.get(key1);
		}
		return rs;
	}

	@Override
	public AreaInfo getAreaInfoFromSp(String spid, long ip) {
		swop();
		AreaInfo rs = null;
		if (spIpRangeMap.containsKey(spid)) {
			TreeMap<Long, AreaInfo> ipRang = spIpRangeMap.get(spid);
			Entry<Long, AreaInfo> entry = ipRang.floorEntry(ip);
			if (entry != null) {
				rs = entry.getValue();
			}
		}
		return rs;
	}

	@Override
	public AreaInfo getAreaInfo(String hid, String oemid, String spid, long ip) {
		swop();
		AreaInfo rs = null;
		try {
			rs = getAreaInfoFromBoxStore(oemid, hid);
			if (rs == null) {
				rs = getAreaInfoFromSp(spid, ip);
			}
			if (rs == null) {
				rs = getAreaInfoNormal(ip);
			}
		} catch (Exception e) {
			logger.warn(getName() + " getAreaInfo error", e);
		}
		return rs;
	}

	@Override
	protected void _swop() {
		boxStoreMap = boxStoreMapSwap;
		spIpRangeMap = spIpRangeMapSwap;
		vooleIpRangeMap = vooleIpRangeMapSwap;
	}

	@Override
	protected void _fetch() {
		try {
			boxStoreMapSwap = getBoxStoreMapFromDb();
			spIpRangeMapSwap = getSpIpRangeMapFromDb();
			vooleIpRangeMapSwap = getVooleIpRangeMapFromDb();
		} catch (Exception e) {
			logger.warn(getName() + " _fetch error", e);
		}
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	public AreaInfosFetch getFetch() {
		return fetch;
	}

	protected TreeMap<Long, AreaInfo> getVooleIpRangeMapFromDb() {
		List<IpRange> ipRanges = getFetch().getVooleIpRanges();
		return analyzeIpRange(ipRanges);
	}

	protected Map<Tuple<String, String>, AreaInfo> getBoxStoreMapFromDb() {
		Map<Tuple<String, String>, AreaInfo> boxStoreMap = new HashMap<Tuple<String, String>, AreaInfo>();
		List<BoxStoreAreaInfo> areaInfo1 = getFetch()
				.getLiveBoxStoreAreaInfos();
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
		Map<String, List<IpRange>> spidToIpRanges = getFetch().getSpIpRanges();
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

}
