/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.cache.HobbitCache.AbstractHobbitCache;
import com.voole.hobbit2.cache.entity.ProductInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ProductB2BInfoCacheImpl extends AbstractHobbitCache implements
		ProductB2BInfoCache {
	private final ProductInfoFetch fetch;

	private volatile Map<String, ProductInfo> productInfoMap;
	private volatile Map<String, ProductInfo> productInfoMapSwap;

	private final Function<String, Optional<ProductInfo>> getProductInfoFunction;

	public ProductB2BInfoCacheImpl(ProductInfoFetch fetch) {
		this.fetch = fetch;
		this.getProductInfoFunction = new Function<String, Optional<ProductInfo>>() {
			@Override
			public Optional<ProductInfo> apply(String product) {
				ProductInfo info = productInfoMap.get(product);
				if (info != null) {
					return Optional.of(info);
				} else {
					return Optional.absent();
				}
			}
		};
	}

	@Override
	public Optional<ProductInfo> getProductB2BInfo(String dim_product_pid)
			throws CacheRefreshException, CacheQueryException {
		return query(getProductInfoFunction, dim_product_pid);
	}

	@Override
	protected void swop() {
		productInfoMap = productInfoMapSwap;
	}

	@Override
	protected void fetch() {
		productInfoMapSwap = ImmutableMap.copyOf(getFetch()
				.getProductB2BInfos());

	}

	public ProductInfoFetch getFetch() {
		return fetch;
	}

}
