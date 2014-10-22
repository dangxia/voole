/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Optional;
import com.voole.hobbit2.cache.entity.ProductInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface ProductInfoCache extends HobbitCache {
	public Optional<ProductInfo> getProductInfo(String dim_po_id, String dim_product_pid)
			throws CacheRefreshException, CacheQueryException;

	public static interface ProductInfoFetch {
		public Map<Tuple<String, String>, ProductInfo> getProductInfos();
	}
}
