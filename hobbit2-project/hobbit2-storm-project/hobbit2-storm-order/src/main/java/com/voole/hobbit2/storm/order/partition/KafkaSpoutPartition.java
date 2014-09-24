/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import storm.trident.spout.ISpoutPartition;

import com.google.common.base.Objects;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class KafkaSpoutPartition implements ISpoutPartition, Serializable,
		Comparable<KafkaSpoutPartition> {
	private BrokerAndTopicPartition brokerAndTopicPartition;
	private long offset;
	private final List<String> noendPaths;

	public KafkaSpoutPartition() {
		noendPaths = new ArrayList<String>();
	}

	@Override
	public int hashCode() {
		return brokerAndTopicPartition.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj != null && obj instanceof KafkaSpoutPartition) {
			return brokerAndTopicPartition
					.equals(((KafkaSpoutPartition) obj).brokerAndTopicPartition);
		}
		return false;
	}

	@Override
	public String getId() {
		return brokerAndTopicPartition.toString();
	}

	public BrokerAndTopicPartition getBrokerAndTopicPartition() {
		return brokerAndTopicPartition;
	}

	public void setBrokerAndTopicPartition(
			BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
	}

	public List<String> getNoendPaths() {
		return noendPaths;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("brokerAndTopicPartition", brokerAndTopicPartition)
				.add("offset", offset).add("noendPaths", noendPaths).toString();
	}

	@Override
	public int compareTo(KafkaSpoutPartition that) {
		return this.brokerAndTopicPartition
				.compareTo(that.brokerAndTopicPartition);
	}

}
