/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.ProductInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface ProductB2BInfoCache extends HobbitCache {
	public Optional<ProductInfo> getProductB2BInfo(String dim_product_pid)
			throws CacheRefreshException, CacheQueryException;

	public static interface ProductInfoFetch {
		public Map<String, ProductInfo> getProductB2BInfos();
	}
}
