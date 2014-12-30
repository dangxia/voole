/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class AreaInfoCacheImpl extends AbstractHobbitCache implements
		AreaInfoCache {
	private final AreaInfosFetch fetch;

	// OEMID HID=>AREAINFO
	// private volatile Map<Tuple<String, String>, AreaInfo> boxStoreMap;
	// SPID=>(IP=>AREEINFO)
	private volatile Map<String, RangeMap<Long, AreaInfo>> spIpRangeMap;
	// IP=>AREAINFO
	private volatile RangeMap<Long, AreaInfo> vooleIpRangeMap;

	// private volatile Map<Tuple<String, String>, AreaInfo> boxStoreMapSwap;
	private volatile Map<String, RangeMap<Long, AreaInfo>> spIpRangeMapSwap;
	private volatile RangeMap<Long, AreaInfo> vooleIpRangeMapSwap;

	private final Function<Long, Optional<AreaInfo>> getAreaInfoNormalFunction;
	// private final Function<Tuple<String, String>, Optional<AreaInfo>>
	// getAreaInfoFromBoxStoreFunction;
	private final Function<Tuple<String, Long>, Optional<AreaInfo>> getAreaInfoFromSpFunction;

	public AreaInfoCacheImpl(AreaInfosFetch fetch) {
		this.fetch = fetch;

		this.getAreaInfoNormalFunction = new Function<Long, Optional<AreaInfo>>() {
			@Override
			public Optional<AreaInfo> apply(Long ip) {
				AreaInfo areaInfo = vooleIpRangeMap.get(ip);
				if (areaInfo != null) {
					return Optional.of(areaInfo);
				}
				return Optional.absent();
			}
		};

		// this.getAreaInfoFromBoxStoreFunction = new Function<Tuple<String,
		// String>, Optional<AreaInfo>>() {
		//
		// @Override
		// public Optional<AreaInfo> apply(Tuple<String, String> key) {
		// if (boxStoreMap.containsKey(key)) {
		// return Optional.of(boxStoreMap.get(key));
		// }
		// return Optional.absent();
		// }
		// };

		this.getAreaInfoFromSpFunction = new Function<Tuple<String, Long>, Optional<AreaInfo>>() {

			@Override
			public Optional<AreaInfo> apply(Tuple<String, Long> input) {
				String spid = input.getA();
				Long ip = input.getB();
				if (spIpRangeMap.containsKey(spid)) {
					RangeMap<Long, AreaInfo> ipRang = spIpRangeMap.get(spid);
					AreaInfo areaInfo = ipRang.get(ip);
					if (areaInfo != null) {
						return Optional.of(areaInfo);
					}
				}
				return Optional.absent();
			}
		};
	}

	@Override
	public Optional<AreaInfo> getAreaInfoNormal(Long ip)
			throws CacheRefreshException, CacheQueryException {
		return query(getAreaInfoNormalFunction, ip);
	}

	// @Override
	// public Optional<AreaInfo> getAreaInfoFromBoxStore(String oemid, String
	// hid)
	// throws CacheRefreshException, CacheQueryException {
	// if (hid != null) {
	// hid = hid.toUpperCase();
	// }
	// return query(this.getAreaInfoFromBoxStoreFunction,
	// new Tuple<String, String>(oemid, hid));
	// }

	@Override
	public Optional<AreaInfo> getAreaInfoFromSp(String spid, Long ip)
			throws CacheRefreshException, CacheQueryException {
		return query(getAreaInfoFromSpFunction, new Tuple<String, Long>(spid,
				ip));
	}

	@Override
	public Optional<AreaInfo> getAreaInfo(String spid, Long ip)
			throws CacheRefreshException, CacheQueryException {
		Optional<AreaInfo> rs = getAreaInfoFromSp(spid, ip);
		if (!rs.isPresent()) {
			rs = getAreaInfoNormal(ip);
		}
		return rs;
	}

	@Override
	protected void swop() {
		// boxStoreMap = boxStoreMapSwap;
		spIpRangeMap = spIpRangeMapSwap;
		vooleIpRangeMap = vooleIpRangeMapSwap;

		// boxStoreMapSwap = null;
		spIpRangeMapSwap = null;
		vooleIpRangeMapSwap = null;
	}

	@Override
	protected void fetch() {
		// boxStoreMapSwap = ImmutableMap.copyOf(getBoxStoreMapFromDb());
		spIpRangeMapSwap = ImmutableMap.copyOf(getFetch().getSpIpRanges());
		vooleIpRangeMapSwap = getFetch().getVooleIpRanges();
	}

	public AreaInfosFetch getFetch() {
		return fetch;
	}

	// protected Map<Tuple<String, String>, AreaInfo> getBoxStoreMapFromDb() {
	// Map<Tuple<String, String>, AreaInfo> boxStoreMap = new
	// HashMap<Tuple<String, String>, AreaInfo>();
	// List<BoxStoreAreaInfo> areaInfo1 = getFetch()
	// .getLiveBoxStoreAreaInfos();
	// for (BoxStoreAreaInfo areaInfo : areaInfo1) {
	// String mac = areaInfo.getMac();
	// if (mac != null) {
	// mac = mac.toUpperCase();
	// }
	// boxStoreMap.put(
	// new Tuple<String, String>(areaInfo.getOemid(), mac),
	// new AreaInfo(areaInfo.getAreaid(), areaInfo.getNettype()));
	// }
	// return boxStoreMap;
	// }

}
