/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit.cache.entity.ResourceInfo;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ResourceInfoCacheImpl extends AbstractHobbitCache implements
		ResourceInfoCache {
	private static final Logger logger = LoggerFactory
			.getLogger(ResourceInfoCacheImpl.class);
	private final ResourceInfoFetch fetch;
	// spid=>movie_spid
	private Map<String, String> spidToMovieSpid;
	// (movie_spid,fid)=>ResourceInfo
	private Map<Tuple<String, String>, ResourceInfo> resourceMap;

	private Map<String, String> spidToMovieSpidSwap;
	private Map<Tuple<String, String>, ResourceInfo> resourceMapSwap;

	public ResourceInfoCacheImpl(ResourceInfoFetch fetch) {
		super("resource-info-cache");
		this.fetch = fetch;
		refreshImmediately();
	}

	@Override
	public ResourceInfo getResourceInfo(String spid, String fid) {
		if (spid == null || fid == null) {
			return null;
		}
		fid = fid.toUpperCase();
		ResourceInfo info = null;
		try {
			String movieSpid = spidToMovieSpid.get(spid);
			if (movieSpid != null) {
				info = resourceMap
						.get(new Tuple<String, String>(movieSpid, fid));
			}
		} catch (Exception e) {
			getLogger().warn(getName() + " getBitrate error", e);
		}
		return info;
	}

	@Override
	protected void _swop() {
		spidToMovieSpid = spidToMovieSpidSwap;
		resourceMap = resourceMapSwap;
	}

	@Override
	protected void _fetch() {
		try {
			spidToMovieSpidSwap = getFetch().getSpidToMovieSpidMap();
			resourceMapSwap = getFetch().getResourceMap();
		} catch (Exception e) {
			getLogger().warn(getName() + " doRefresh error", e);
		}
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	public ResourceInfoFetch getFetch() {
		return fetch;
	}

}
