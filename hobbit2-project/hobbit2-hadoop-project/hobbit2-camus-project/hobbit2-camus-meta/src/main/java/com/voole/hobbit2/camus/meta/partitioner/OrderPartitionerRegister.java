/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.partitioner;

import com.voole.hobbit2.kafka.avro.order.util.OrderTopicsUtils;
import com.voole.hobbit2.kafka.common.partition.PartitionerRegister;
import com.voole.hobbit2.kafka.common.partition.Partitioners;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class OrderPartitionerRegister implements PartitionerRegister {

	@Override
	public void register(Partitioners partitioners) {
		OrderPartitioner partitioner = new OrderPartitioner();
		for (String topic : OrderTopicsUtils.topics) {
			partitioners.register(topic, partitioner);
		}

	}

}
