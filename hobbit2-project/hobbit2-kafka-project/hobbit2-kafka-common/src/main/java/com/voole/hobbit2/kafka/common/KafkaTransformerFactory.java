/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMeta;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMetas;

/**
 * @author XuehuiHe
 * @date 2014年8月18日
 */
public class KafkaTransformerFactory {
	private static final Map<String, KafkaTransformer<?>> cache = new HashMap<String, KafkaTransformer<?>>();
	private static final Set<String> failedTopics = new HashSet<String>();

	public static KafkaTransformer<?> getTransformer(String topic)
			throws KafkaTransformException {
		if (!isCreated(topic)) {
			_create(topic);
		}
		return cache.get(topic);
	}

	private static boolean isCreated(String topic) {
		return cache.containsKey(topic) || failedTopics.contains(topic);
	}

	private static synchronized void _create(String topic)
			throws KafkaTransformException {
		if (isCreated(topic)) {
			return;
		}
		KafkaTopicTransformMeta<?, ?> meta = KafkaTopicTransformMetas.get(topic);
		if (meta != null) {
			KafkaTransformer<?> transformer = meta.createTransformer();
			if (transformer != null) {
				cache.put(topic, transformer);
			} else {
				failedTopics.add(topic);
				throw new KafkaTransformException("topic:" + topic
						+ " transformer init failed!");
			}
		} else {
			failedTopics.add(topic);
			throw new KafkaTransformException("topic:" + topic
					+ " meta not found");
		}
	}
}
