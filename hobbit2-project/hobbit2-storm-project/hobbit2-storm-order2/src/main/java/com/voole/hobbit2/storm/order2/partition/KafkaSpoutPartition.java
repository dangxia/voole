/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order2.partition;

import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class KafkaSpoutPartition extends HdfsKafkaMixedSpoutPartition implements
		Comparable<KafkaSpoutPartition> {

	private BrokerAndTopicPartition brokerAndTopicPartition;

	public KafkaSpoutPartition(BrokerAndTopicPartition brokerAndTopicPartition) {
		super(Type.KAFKA);
		setBrokerAndTopicPartition(brokerAndTopicPartition);
	}

	public KafkaSpoutPartition() {
		super(Type.KAFKA);
	}

	@Override
	public String getId() {
		return getBrokerAndTopicPartition().getPartition().getTopic() + "_"
				+ getBrokerAndTopicPartition().getPartition().getPartition();
	}

	public BrokerAndTopicPartition getBrokerAndTopicPartition() {
		return brokerAndTopicPartition;
	}

	public void setBrokerAndTopicPartition(
			BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
	}

	@Override
	public int hashCode() {
		return getBrokerAndTopicPartition().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return getBrokerAndTopicPartition().equals(obj);
	}

	@Override
	public String toString() {
		return getBrokerAndTopicPartition().toString();
	}

	@Override
	public int compareTo(KafkaSpoutPartition o) {
		return this.getBrokerAndTopicPartition().compareTo(
				o.getBrokerAndTopicPartition());
	}

}
