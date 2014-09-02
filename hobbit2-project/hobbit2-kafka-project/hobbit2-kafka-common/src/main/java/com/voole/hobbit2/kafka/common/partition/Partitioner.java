/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.partition;

import com.voole.hobbit2.kafka.common.IKafkaKey;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface Partitioner<K extends IKafkaKey, V> {
	void partition(K key, V value);

	String getPath(K key);
}
