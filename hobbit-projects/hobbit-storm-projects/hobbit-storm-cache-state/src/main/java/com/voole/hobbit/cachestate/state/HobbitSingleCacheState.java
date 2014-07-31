/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import com.voole.hobbit.cache.HobbitCache;

/**
 * @author XuehuiHe
 * @date 2014年6月25日
 */
public class HobbitSingleCacheState<T extends HobbitCache> extends
		HobbitCompositeCacheState {
	public HobbitSingleCacheState(T cache) {
		super(cache);
	}

	@SuppressWarnings("unchecked")
	public T getCache() {
		return (T) getCaches().get(0);
	}
}
