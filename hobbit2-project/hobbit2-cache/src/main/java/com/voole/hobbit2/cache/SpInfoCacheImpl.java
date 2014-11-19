package com.voole.hobbit2.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.SpInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

public class SpInfoCacheImpl extends AbstractHobbitCache implements SpInfoCache {

	private volatile Map<String, SpInfo> spidToSpInfo;
	private volatile Map<String, SpInfo> spidToSpInfoSwap;

	private volatile Map<Integer, List<SpInfo>> netTypeToSpInfos;
	private volatile Map<Integer, List<SpInfo>> netTypeToSpInfosSwap;

	private final SpInfosFetch fetch;

	private final Function<Void, Collection<SpInfo>> getAllSpInfoFunction;
	private final Function<String, Optional<SpInfo>> getSpInfoFunction;
	private final Function<Integer, List<SpInfo>> getSpInfosFunction;

	public SpInfoCacheImpl(SpInfosFetch fetch) {
		this.fetch = fetch;

		getAllSpInfoFunction = new Function<Void, Collection<SpInfo>>() {
			@Override
			public Collection<SpInfo> apply(Void input) {
				return spidToSpInfo.values();
			}
		};

		getSpInfoFunction = new Function<String, Optional<SpInfo>>() {

			@Override
			public Optional<SpInfo> apply(String spid) {
				if (spidToSpInfo.containsKey(spid)) {
					return Optional.of(spidToSpInfo.get(spid));
				}
				return Optional.absent();
			}
		};

		getSpInfosFunction = new Function<Integer, List<SpInfo>>() {

			@SuppressWarnings("unchecked")
			@Override
			public List<SpInfo> apply(Integer nettype) {
				if (netTypeToSpInfos.containsKey(nettype)) {
					return netTypeToSpInfos.get(nettype);
				}
				return Collections.EMPTY_LIST;
			}
		};
	}

	@Override
	public Collection<SpInfo> getAllSpInfo() throws CacheRefreshException,
			CacheQueryException {
		return query(getAllSpInfoFunction, null);
	}

	@Override
	public Optional<SpInfo> getSpInfo(String spid)
			throws CacheRefreshException, CacheQueryException {
		return query(getSpInfoFunction, spid);
	}

	@Override
	public List<SpInfo> getSpInfos(Integer nettype)
			throws CacheRefreshException, CacheQueryException {
		return query(getSpInfosFunction, nettype);
	}

	@Override
	protected void swop() {
		spidToSpInfo = ImmutableMap.copyOf(spidToSpInfoSwap);
		spidToSpInfoSwap = null;

		netTypeToSpInfos = ImmutableMap.copyOf(netTypeToSpInfosSwap);
		netTypeToSpInfosSwap = null;
	}

	@Override
	protected void fetch() {
		List<SpInfo> spInfos = fetch.getSpInfos();
		spidToSpInfoSwap = new HashMap<String, SpInfo>();
		netTypeToSpInfosSwap = new HashMap<Integer, List<SpInfo>>();
		for (SpInfo spInfo : spInfos) {
			Integer nettype = spInfo.getNettype();
			List<SpInfo> netTypeSpInfos = null;
			if (netTypeToSpInfosSwap.containsKey(nettype)) {
				netTypeSpInfos = netTypeToSpInfosSwap.get(nettype);
			} else {
				netTypeSpInfos = new ArrayList<SpInfo>();
				netTypeToSpInfosSwap.put(nettype, netTypeSpInfos);
			}
			netTypeSpInfos.add(spInfo);
			spidToSpInfoSwap.put(spInfo.getSpid(), spInfo);
		}
	}
}
