/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * 使用不可变Map提高查询效率和线程安全
 * 
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class DefaultTransformerManager implements ICamusTransformerManager{
	private final Map<ICamusKey, ICamusTransformer<?, ?>> transformerMap;

	public DefaultTransformerManager(ICamusTransformerRegister... registers) {
		Map<ICamusKey, ICamusTransformer<?, ?>> temp = new HashMap<ICamusKey, ICamusTransformer<?, ?>>();
		for (ICamusTransformerRegister register : registers) {
			register.add(temp);
		}
		transformerMap = ImmutableMap.copyOf(temp);
	}

	@SuppressWarnings("unchecked")
	public <Source, Target> ICamusTransformer<Source, Target> findTransformer(
			ICamusKey key) {
		Preconditions.checkNotNull(key);
		if (transformerMap.containsKey(key)) {
			return (ICamusTransformer<Source, Target>) transformerMap.get(key);
		}
		throw new UnsupportedOperationException(
				"not found transformer for key:" + key);
	}

	public Map<ICamusKey, ICamusTransformer<?, ?>> getTransformerMap() {
		return transformerMap;
	}

}
