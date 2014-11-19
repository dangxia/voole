/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class OemInfoCacheImpl extends AbstractHobbitCache implements
		OemInfoCache, InitializingBean {
	private final OemInfoFetch fetch;

	private volatile Map<Long, OemInfo> oemInfoMap;
	private volatile Map<Long, OemInfo> oemInfoMapSwap;

	private final Function<Long, Optional<OemInfo>> getOemInfoFunction;

	public OemInfoCacheImpl(OemInfoFetch fetch) {
		this.fetch = fetch;
		this.getOemInfoFunction = new Function<Long, Optional<OemInfo>>() {
			@Override
			public Optional<OemInfo> apply(Long oemid) {
				OemInfo info = oemInfoMap.get(oemid);
				if (info != null) {
					return Optional.of(info);
				} else {
					return Optional.absent();
				}
			}
		};
	}

	@Override
	public Optional<OemInfo> getOemInfo(Long oemid)
			throws CacheRefreshException, CacheQueryException {
		return query(getOemInfoFunction, oemid);
	}

	@Override
	protected void swop() {
		oemInfoMap = oemInfoMapSwap;
	}

	@Override
	protected void fetch() {
		List<OemInfo> list = getFetch().getOemInfos();
		oemInfoMapSwap = new HashMap<Long, OemInfo>();
		for (OemInfo oemInfo : list) {
			oemInfoMapSwap.put(oemInfo.getOemid(), oemInfo);
		}
		oemInfoMapSwap = ImmutableMap.copyOf(oemInfoMapSwap);

	}

	public OemInfoFetch getFetch() {
		return fetch;
	}

}
