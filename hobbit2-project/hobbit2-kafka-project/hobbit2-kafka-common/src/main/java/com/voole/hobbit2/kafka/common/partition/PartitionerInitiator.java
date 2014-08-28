/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.partition;

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface PartitionerInitiator {
	public void initialize(Partitioners partitioners);

	public abstract class DefaultPartitionerInitiator implements
			PartitionerInitiator {
		@Override
		public void initialize(Partitioners partitioners) {
			for (Entry<String, Partitioner<?, ?, ?>> entry : getTopicToPartitionerMap()
					.entrySet()) {
				partitioners.register(entry.getKey(), entry.getValue());
			}
		}

		protected abstract Map<String, Partitioner<?, ?, ?>> getTopicToPartitionerMap();
	}
}
