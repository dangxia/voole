/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.voole.hobbit.kafka.TopicProtoClassUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class KafkaProtoBuffTransformerFactory {
	// public static <T extends GeneratedMessage> KafkaProtoBuffTransformer<T>
	// getTransformer(
	// Class<T> clazz) throws IllegalAccessException,
	// IllegalArgumentException, InvocationTargetException,
	// NoSuchMethodException, SecurityException, ClassNotFoundException {
	// return new KafkaTerminalProtoBuffTransformer<T>(clazz);
	// }

	private static final Map<String, KafkaProtoBuffTransformer> map = new HashMap<String, KafkaProtoBuffTransformer>();

	public static synchronized KafkaProtoBuffTransformer getTransformer(
			String topic) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (!map.containsKey(topic)) {
			map.put(topic, new KafkaTerminalProtoBuffTransformer(
					TopicProtoClassUtils.topicMapProtoClass.get(topic)));
		}
		return map.get(topic);
	}
}
