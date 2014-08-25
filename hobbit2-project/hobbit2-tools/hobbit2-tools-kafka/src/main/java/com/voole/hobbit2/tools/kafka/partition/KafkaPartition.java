/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.Serializable;

import kafka.common.TopicAndPartition;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaPartition implements Serializable {
	private final Broker broker;
	private final int partition;
	private final String topic;

	public KafkaPartition(Broker broker, String topic, int partition) {
		this.broker = broker;
		this.partition = partition;
		this.topic = topic;
	}

	public Broker getBroker() {
		return broker;
	}

	public int getPartition() {
		return partition;
	}

	public String getTopic() {
		return topic;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("broker", broker)
				.add("topic", topic).add("partition", partition).toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof KafkaPartition) {
			KafkaPartition that = (KafkaPartition) obj;
			return Objects.equal(this.getBroker(), that.getBroker())
					&& Objects.equal(this.getPartition(), that.getPartition())
					&& Objects.equal(this.getTopic(), that.getTopic());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.getBroker(), this.getTopic(),
				this.getPartition());
	}

	public TopicAndPartition getTopicAndPartition() {
		return new TopicAndPartition(topic, partition);
	}

	public KafkaPartitionState createState() {
		return new KafkaPartitionState(this);
	}

}
