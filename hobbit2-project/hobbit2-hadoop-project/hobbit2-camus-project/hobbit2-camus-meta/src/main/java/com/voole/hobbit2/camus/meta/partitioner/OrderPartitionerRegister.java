/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.partitioner;

import com.voole.hobbit2.kafka.avro.order.util.AbstractOrderPartitionerRegister;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class OrderPartitionerRegister extends AbstractOrderPartitionerRegister {

	@Override
	protected AbstractOrderPartitioner<?, ?> getOrderPartitioner() {
		return new OrderPartitioner();
	}

}
