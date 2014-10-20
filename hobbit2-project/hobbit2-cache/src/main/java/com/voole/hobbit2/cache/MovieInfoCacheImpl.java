/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.MovieInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class MovieInfoCacheImpl extends AbstractHobbitCache implements
		MovieInfoCache {
	private final MovieInfoFetch fetch;
	private volatile Map<Long, MovieInfo> movieMap;
	private volatile Map<Long, MovieInfo> movieMapSwap;
	private final Function<Long, Optional<MovieInfo>> getMovieInfoFunction;
	public MovieInfoCacheImpl(MovieInfoFetch fetch) {
		this.fetch = fetch;
		this.getMovieInfoFunction = new Function<Long, Optional<MovieInfo>>() {
			@Override
			public Optional<MovieInfo> apply(Long mid) {
				MovieInfo info = null;
				info = movieMap.get(mid);
				if (info != null) {
					return Optional.of(info);
				}
				return Optional.absent();
			}
		};
	}

	@Override
	public Optional<MovieInfo> getMovieInfo(Long mid)
			throws CacheRefreshException, CacheQueryException {
		if (mid == null || mid == null) {
			return Optional.absent();
		}
		return query(this.getMovieInfoFunction, mid);
	}

	@Override
	protected void swop() {
		movieMap = movieMapSwap;
	}

	@Override
	protected void fetch() {
		movieMapSwap = ImmutableMap.copyOf(getFetch().getMovieMap());
	}

	public MovieInfoFetch getFetch() {
		return fetch;
	}

}
