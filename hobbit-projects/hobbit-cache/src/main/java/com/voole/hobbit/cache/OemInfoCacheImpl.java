/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit.cache.entity.OemInfo;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class OemInfoCacheImpl extends AbstractHobbitCache implements
		OemInfoCache {
	private static final Logger logger = LoggerFactory
			.getLogger(OemInfoCacheImpl.class);
	private final OemInfoFetch fetch;

	private Map<Long, OemInfo> oemInfoMap;
	private Map<Long, OemInfo> oemInfoMapSwap;

	public OemInfoCacheImpl(OemInfoFetch fetch) {
		super("oeminfo-cache");
		this.fetch = fetch;
		refreshImmediately();
	}

	@Override
	public OemInfo getOemInfo(Long oemid) {
		swop();
		OemInfo info = null;
		try {
			info = oemInfoMap.get(oemid);
		} catch (Exception e) {
			getLogger().warn(getName() + " getOemInfo error", e);
		}
		return info;
	}

	@Override
	protected void _swop() {
		oemInfoMap = oemInfoMapSwap;
	}

	@Override
	protected void _fetch() {
		try {
			List<OemInfo> list = getFetch().getOemInfos();
			oemInfoMapSwap = new HashMap<Long, OemInfo>();
			for (OemInfo oemInfo : list) {
				oemInfoMapSwap.put(oemInfo.getOemid(), oemInfo);
			}
		} catch (Exception e) {
			getLogger().warn(getName() + " _fetch error", e);
		}

	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	public OemInfoFetch getFetch() {
		return fetch;
	}

}
