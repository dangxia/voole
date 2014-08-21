/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.util.HashMap;
import java.util.Map;

import com.voole.hobbit.util.AvroUtils;
import com.voole.hobbit.util.TopicUtils;

/**
 * @author XuehuiHe
 * @date 2014年8月18日
 */
public class KafkaTransformerFactory {
	public static final Map<String, KafkaTransformer> cache = new HashMap<String, KafkaTransformer>();

	public static KafkaTransformer getTransformer(String topic) {
		if (!cache.containsKey(topic)) {
			_create(topic);

		}
		return cache.get(topic);
	}

	public static synchronized void _create(String topic) {
		if (cache.containsKey(topic)) {
			return;
		}
		if (TopicUtils.ORDER_TOPICS.contains(topic)) {
			cache.put(
					topic,
					new KafkaTerminalAvroTransformer(AvroUtils
							.getKafkaTopicSchema(topic)));
		} else {
			cache.put(topic, null);
		}
	}
}
