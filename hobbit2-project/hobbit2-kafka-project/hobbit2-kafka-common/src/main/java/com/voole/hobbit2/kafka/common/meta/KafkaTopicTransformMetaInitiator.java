/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.meta;

import java.util.Collection;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface KafkaTopicTransformMetaInitiator {
	public void initialize(KafkaTopicTransformMetas kafkaTopicTransformMetas);

	public abstract class DefaultKafkaTopicTransformMetaInitiator implements
			KafkaTopicTransformMetaInitiator {
		public DefaultKafkaTopicTransformMetaInitiator() {
		}

		@Override
		public void initialize(KafkaTopicTransformMetas kafkaTopicTransformMetas) {
			for (KafkaTopicTransformMeta<?, ?> item : getTopicToTransformMetaMap()) {
				kafkaTopicTransformMetas.register(item);
			}
		}

		protected abstract Collection<? extends KafkaTopicTransformMeta<?, ?>> getTopicToTransformMetaMap();
	}
}
