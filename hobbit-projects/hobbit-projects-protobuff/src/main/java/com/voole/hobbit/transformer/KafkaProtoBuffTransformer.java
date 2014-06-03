/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.GeneratedMessage;

/**
 * @author XuehuiHe
 * @date 2014年5月30日
 */
public interface KafkaProtoBuffTransformer<T extends GeneratedMessage> {
	T transform(byte[] bytes) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException;
}
