package com.voole.hobbit2.cache;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.SpInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

public interface SpInfoCache {
	public Collection<SpInfo> getAllSpInfo() throws CacheRefreshException,
			CacheQueryException;

	public Optional<SpInfo> getSpInfo(String spid)
			throws CacheRefreshException, CacheQueryException;

	public List<SpInfo> getSpInfos(Integer nettype)
			throws CacheRefreshException, CacheQueryException;

	public static interface SpInfosFetch {
		public List<SpInfo> getSpInfos();
	}
}
