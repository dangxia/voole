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
public interface PartitionerRegister {
	public void register(Partitioners partitioners);

	public abstract class DefaultPartitionerRegister implements
			PartitionerRegister {
		@Override
		public void register(Partitioners partitioners) {
			for (Entry<String, Partitioner<?, ?, ?>> entry : getTopicToPartitionerMap()
					.entrySet()) {
				partitioners.register(entry.getKey(), entry.getValue());
			}
		}

		protected abstract Map<String, Partitioner<?, ?, ?>> getTopicToPartitionerMap();
	}
}
