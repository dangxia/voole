/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.ResourceInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ResourceInfoCacheImpl extends AbstractHobbitCache implements
		ResourceInfoCache {
	private final ResourceInfoFetch fetch;
	// spid=>movie_spid
	private volatile Map<String, String> spidToMovieSpid;
	// (movie_spid,fid)=>ResourceInfo
	private volatile Map<String, ResourceInfo> resourceMap;

	private volatile Map<String, String> spidToMovieSpidSwap;
	private volatile Map<String, ResourceInfo> resourceMapSwap;

	private final Function<String, Optional<ResourceInfo>> getResourceInfoFunction;

	public ResourceInfoCacheImpl(ResourceInfoFetch fetch) {
		this.fetch = fetch;
		this.getResourceInfoFunction = new Function<String, Optional<ResourceInfo>>() {
			@Override
			public Optional<ResourceInfo> apply(String fid) {
				fid = fid.toUpperCase();
				ResourceInfo info = null;
				//String movieSpid = spidToMovieSpid.get(spid);
					info = resourceMap.get(fid);
				if (info != null) {
					return Optional.of(info);
				}
				return Optional.absent();
			}
		};
	}

	@Override
	public Optional<ResourceInfo> getResourceInfo(String spid, String fid)
			throws CacheRefreshException, CacheQueryException {
		if (spid == null || fid == null) {
			return Optional.absent();
		}
		return query(this.getResourceInfoFunction, fid);
	}

	@Override
	protected void swop() {
		//spidToMovieSpid = spidToMovieSpidSwap;
		resourceMap = resourceMapSwap;
	}

	@Override
	protected void fetch() {
		//spidToMovieSpidSwap = ImmutableMap.copyOf(getFetch().getSpidToMovieSpidMap());
		resourceMapSwap = ImmutableMap.copyOf(getFetch().getResourceMap());
	}

	public ResourceInfoFetch getFetch() {
		return fetch;
	}

}
