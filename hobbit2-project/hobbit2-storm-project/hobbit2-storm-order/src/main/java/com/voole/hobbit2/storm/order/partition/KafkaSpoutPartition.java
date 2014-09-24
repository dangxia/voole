/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.Serializable;

import storm.trident.spout.ISpoutPartition;

import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class KafkaSpoutPartition extends BrokerAndTopicPartition implements
		ISpoutPartition, Serializable {

	public KafkaSpoutPartition() {
	}

	public KafkaSpoutPartition(BrokerAndTopicPartition that) {
		super(that.getBroker(), that.getPartition());
	}

	@Override
	public String getId() {
		return getPartition().toString();
	}
}
