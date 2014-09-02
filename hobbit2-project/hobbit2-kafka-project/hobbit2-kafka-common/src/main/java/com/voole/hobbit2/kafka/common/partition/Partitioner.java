/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.partition;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface Partitioner<P, K, V> {
	void partition(P p, K kafkakey, V value);

	String getPath(P p);
}
