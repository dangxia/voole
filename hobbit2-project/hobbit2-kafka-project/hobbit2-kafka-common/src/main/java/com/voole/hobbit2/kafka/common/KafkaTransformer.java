/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common;

import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public interface KafkaTransformer<T> {
	T transform(byte[] bytes) throws KafkaTransformException;

	T transform(String str) throws KafkaTransformException;
}
