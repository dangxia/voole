/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import kafka.common.TopicAndPartition;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class BrokerAndTopicPartition implements Serializable, Writable {
	private Broker broker;
	private TopicPartition partition;

	public BrokerAndTopicPartition() {
	}

	public BrokerAndTopicPartition(Broker broker, String topic, int partition) {
		this(broker, new TopicPartition(topic, partition));
	}

	public BrokerAndTopicPartition(Broker broker, TopicPartition partition) {
		this.broker = broker;
		this.partition = partition;
	}

	public Broker getBroker() {
		return broker;
	}

	public TopicPartition getPartition() {
		return partition;
	}

	public void setBroker(Broker broker) {
		this.broker = broker;
	}

	public void setPartition(TopicPartition partition) {
		this.partition = partition;
	}

	@Override
	public String toString() {
		return getToStringHelper().toString();
	}

	protected ToStringHelper getToStringHelper() {
		return Objects.toStringHelper(this).add("broker", broker)
				.add("topic_partition", partition);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof BrokerAndTopicPartition) {
			BrokerAndTopicPartition that = (BrokerAndTopicPartition) obj;
			return Objects.equal(this.getBroker(), that.getBroker())
					&& Objects.equal(this.getPartition(), that.getPartition());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.getBroker(), this.getPartition());
	}

	public TopicAndPartition getTopicAndPartition() {
		return new TopicAndPartition(partition.getTopic(),
				partition.getPartition());
	}

	public PartitionState createState() {
		return new PartitionState(this);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.broker.write(out);
		this.partition.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.broker == null) {
			this.broker = new Broker();
		}
		if (this.partition == null) {
			this.partition = new TopicPartition();
		}
		this.broker.readFields(in);
		this.partition.readFields(in);
	}

}
