/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.meta;

import java.util.HashMap;
import java.util.Map;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class KafkaTopicTransformMetas {
	private static class KafkaTopicMetasHolder {
		private static KafkaTopicTransformMetas _s = new KafkaTopicTransformMetas();
	}

	private final Map<String, KafkaTopicTransformMeta<?, ?>> map;

	private KafkaTopicTransformMetas() {
		map = new HashMap<String, KafkaTopicTransformMeta<?, ?>>();
	}

	public synchronized KafkaTopicTransformMeta<?, ?> _get(String topic) {
		if (map.containsKey(topic)) {
			return map.get(topic);
		}
		return null;
	}

	public synchronized void _register(KafkaTopicTransformMeta<?, ?> meta) {
		map.put(meta.getTopic(), meta);
	}

	public synchronized void _unregister(String topic) {
		map.remove(topic);
	}

	public static synchronized KafkaTopicTransformMetas getInstance() {
		return KafkaTopicMetasHolder._s;
	}

	public static synchronized void register(KafkaTopicTransformMeta<?, ?> meta) {
		getInstance()._register(meta);
	}

	public static synchronized void unregister(String topic) {
		getInstance()._unregister(topic);
	}

	public static synchronized KafkaTopicTransformMeta<?, ?> get(String topic) {
		return getInstance()._get(topic);
	}
}
