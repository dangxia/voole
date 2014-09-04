/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.partitioner;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class DefaultPartitionerManager implements ICamusPartitionerManager {
	private HourlyPartitioner defaultPartitioner;
	private Map<String, ICamusPartitioner> partitionerMap;

	public DefaultPartitionerManager() {
		partitionerMap = new HashMap<String, ICamusPartitioner>();
		defaultPartitioner = new HourlyPartitioner();
	}

	@Override
	public ICamusPartitioner findPartitioner(String topic) {
		if (partitionerMap.containsKey(topic)) {
			return partitionerMap.get(topic);
		}
		return defaultPartitioner;
	}

	@Override
	public void register(String topic, ICamusPartitioner partitioner) {
		Preconditions.checkNotNull(topic);
		partitionerMap.put(topic, partitioner);
	}

	@Override
	public void encapsulate() {
		partitionerMap = ImmutableMap.copyOf(partitionerMap);
	}

}
