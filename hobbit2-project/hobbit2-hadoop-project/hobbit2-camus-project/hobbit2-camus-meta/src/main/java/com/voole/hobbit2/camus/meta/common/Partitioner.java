/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.common;


/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public interface Partitioner<P, V> {
	void partition(P p, CamusKafkaKey kafkakey, V value);
}
