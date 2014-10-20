/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.ParentAreaInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ParentAreaInfoCacheImpl extends AbstractHobbitCache implements
		ParentAreaInfoCache {
	private final ParentAreaInfoFetch fetch;
	private volatile Map<Integer, ParentAreaInfo> parentAreaMap;
	private volatile Map<Integer, ParentAreaInfo> parentAreaMapSwap;
	private final Function<Integer, Optional<ParentAreaInfo>> getParentAreaInfoFunction;

	public ParentAreaInfoCacheImpl(ParentAreaInfoFetch fetch) {
		this.fetch = fetch;
		this.getParentAreaInfoFunction = new Function<Integer, Optional<ParentAreaInfo>>() {
			@Override
			public Optional<ParentAreaInfo> apply(Integer areaid) {
				ParentAreaInfo info = null;
				info = parentAreaMap.get(areaid);
				if (info != null) {
					return Optional.of(info);
				}
				return Optional.absent();
			}
		};
	}

	@Override
	public Optional<ParentAreaInfo> getParentAreaInfo(Integer areaid)
			throws CacheRefreshException, CacheQueryException {
		if (areaid == null || areaid == null) {
			return Optional.absent();
		}
		return query(this.getParentAreaInfoFunction, areaid);
	}

	@Override
	protected void swop() {
		parentAreaMap = parentAreaMapSwap;
	}

	@Override
	protected void fetch() {
		parentAreaMapSwap = ImmutableMap.copyOf(getFetch().getParentAreaMap());
	}

	public ParentAreaInfoFetch getFetch() {
		return fetch;
	}

}
