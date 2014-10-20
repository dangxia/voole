/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.ParentSectionInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ParentSectionInfoCacheImpl extends AbstractHobbitCache implements
		ParentSectionInfoCache {
	private final ParentSectionInfoFetch fetch;
	private volatile Map<String, ParentSectionInfo> parentSectionMap;
	private volatile Map<String, ParentSectionInfo> parentSectionMapSwap;
	private final Function<String, Optional<ParentSectionInfo>> getParentSectionInfoFunction;

	public ParentSectionInfoCacheImpl(ParentSectionInfoFetch fetch) {
		this.fetch = fetch;
		this.getParentSectionInfoFunction = new Function<String, Optional<ParentSectionInfo>>() {
			@Override
			public Optional<ParentSectionInfo> apply(String sectionid) {
				ParentSectionInfo info = null;
				info = parentSectionMap.get(sectionid);
				if (info != null) {
					return Optional.of(info);
				}
				return Optional.absent();
			}
		};
	}

	@Override
	public Optional<ParentSectionInfo> getParentSectionInfo(String sectionid)
			throws CacheRefreshException, CacheQueryException {
		if (sectionid == null || sectionid == null) {
			return Optional.absent();
		}
		return query(this.getParentSectionInfoFunction, sectionid);
	}

	@Override
	protected void swop() {
		parentSectionMap = parentSectionMapSwap;
	}

	@Override
	protected void fetch() {
		parentSectionMapSwap = ImmutableMap.copyOf(getFetch()
				.getParentSectionMap());
	}

	public ParentSectionInfoFetch getFetch() {
		return fetch;
	}

}
