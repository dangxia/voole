/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.GeneratedMessage;
import com.voole.hobbit.kafka.TopicProtoClassUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class KafkaProtoBuffTransformerFactory {
	public static <T extends GeneratedMessage> KafkaProtoBuffTransformer<T> newTransformer(
			Class<T> clazz) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		return new KafkaTerminalProtoBuffTransformer<T>(clazz);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static KafkaProtoBuffTransformer newTransformer(String topic)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException, ClassNotFoundException {
		return new KafkaTerminalProtoBuffTransformer(
				TopicProtoClassUtils.topicMapProtoClass.get(topic));
	}
}
