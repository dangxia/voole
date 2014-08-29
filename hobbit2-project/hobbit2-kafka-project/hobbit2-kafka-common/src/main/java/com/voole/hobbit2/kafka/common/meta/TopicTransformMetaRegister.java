/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.meta;

import java.util.Collection;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface TopicTransformMetaRegister {
	public void register(TopicTransformMetas topicTransformMetas);

	public abstract class DefaultTopicTransformMetaInitiator implements
			TopicTransformMetaRegister {
		public DefaultTopicTransformMetaInitiator() {
		}

		@Override
		public void register(TopicTransformMetas topicTransformMetas) {
			for (TopicTransformMeta<?, ?> item : getTopicToTransformMetaMap()) {
				topicTransformMetas.register(item);
			}
		}

		protected abstract Collection<? extends TopicTransformMeta<?, ?>> getTopicToTransformMetaMap();
	}
}
