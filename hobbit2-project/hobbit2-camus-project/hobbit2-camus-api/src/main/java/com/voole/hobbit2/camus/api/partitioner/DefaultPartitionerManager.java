/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.partitioner;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class DefaultPartitionerManager implements ICamusPartitionerManager {
	private final Map<ICamusKey, ICamusPartitioner<?, ?>> partitionerMap;

	public DefaultPartitionerManager(ICamusPartitionerRegister... registers) {
		Map<ICamusKey, ICamusPartitioner<?, ?>> tempPartitionerMap = new HashMap<ICamusKey, ICamusPartitioner<?, ?>>();
		for (ICamusPartitionerRegister register : registers) {
			register.add(tempPartitionerMap);
		}
		partitionerMap = ImmutableMap.copyOf(tempPartitionerMap);
	}

	@SuppressWarnings("unchecked")
	public <Key extends ICamusKey, Value> ICamusPartitioner<Key, Value> findPartitioner(
			Key key, Value value) {
		if (partitionerMap.containsKey(key)) {
			return (ICamusPartitioner<Key, Value>) partitionerMap.get(key);
		}
		throw new UnsupportedOperationException(
				"not found partitioner for key:" + key);
	}
}
