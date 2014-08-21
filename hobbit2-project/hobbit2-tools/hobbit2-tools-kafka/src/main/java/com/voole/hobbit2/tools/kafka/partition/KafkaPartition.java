/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.Serializable;

import kafka.common.TopicAndPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaPartition implements Serializable {
	private Broker broker;
	private int partition;
	private String topic;
	private long latestOffset;
	private long earliestOffset;

	public KafkaPartition() {
	}

	public KafkaPartition(Broker broker, String topic, int partition) {
		this.broker = broker;
		this.partition = partition;
		this.topic = topic;
	}

	public Broker getBroker() {
		return broker;
	}

	public void setBroker(Broker broker) {
		this.broker = broker;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getLatestOffset() {
		return latestOffset;
	}

	public void setLatestOffset(long latestOffset) {
		this.latestOffset = latestOffset;
	}

	public long getEarliestOffset() {
		return earliestOffset;
	}

	public void setEarliestOffset(long earliestOffset) {
		this.earliestOffset = earliestOffset;
	}

	@Override
	public String toString() {
		return getBroker().toString() + "\ttopic:" + getTopic()
				+ "\tpartition:" + getPartition() + "\tlatestOffset:"
				+ getLatestOffset() + "\tearliestOffset:" + getEarliestOffset();
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
			return this.getBroker().equals(that.getBroker())
					&& this.getPartition() == that.getPartition()
					&& this.getTopic().equals(that.getTopic());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.getBroker().hashCode() + this.getPartition() * 17
				+ this.getTopic().hashCode() * 7;
	}

	public TopicAndPartition getTopicAndPartition() {
		return new TopicAndPartition(topic, partition);
	}

}
