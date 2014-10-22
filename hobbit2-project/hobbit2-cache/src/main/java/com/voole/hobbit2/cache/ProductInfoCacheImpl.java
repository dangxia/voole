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
import com.voole.hobbit2.common.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class ProductInfoCacheImpl extends AbstractHobbitCache implements
		ProductInfoCache {
	private final ProductInfoFetch fetch;

	private volatile Map<Tuple<String, String>, ProductInfo> productInfoMap;
	private volatile Map<Tuple<String, String>, ProductInfo> productInfoMapSwap;

	private final Function<Tuple<String, String>, Optional<ProductInfo>> getProductInfoFunction;

	public ProductInfoCacheImpl(ProductInfoFetch fetch) {
		this.fetch = fetch;
		this.getProductInfoFunction = new Function<Tuple<String, String>, Optional<ProductInfo>>() {
			@Override
			public Optional<ProductInfo> apply(Tuple<String, String> product) {
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
	public Optional<ProductInfo> getProductInfo(String dim_po_id, String dim_product_pid)
			throws CacheRefreshException, CacheQueryException {
		return query(getProductInfoFunction, new Tuple<String, String>(dim_po_id, dim_product_pid));
	}

	@Override
	protected void swop() {
		productInfoMap = productInfoMapSwap;
	}

	@Override
	protected void fetch() {
		productInfoMapSwap = ImmutableMap.copyOf(getFetch().getProductInfos());

	}

	public ProductInfoFetch getFetch() {
		return fetch;
	}

}
