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
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class TopicPartition implements Serializable, Writable {
	private int partition;
	private String topic;

	public TopicPartition() {
	}

	public TopicPartition(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}

	public int getPartition() {
		return partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.topic, this.partition);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (obj instanceof TopicPartition) {
			TopicPartition that = (TopicPartition) obj;
			return Objects.equal(this.topic, that.topic)
					&& Objects.equal(this.partition, that.partition);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("topic", topic)
				.add("partition", partition).toString();
	}

	public TopicAndPartition getTopicAndPartition() {
		return new TopicAndPartition(topic, partition);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, getTopic());
		WritableUtils.writeVInt(out, getPartition());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.topic = WritableUtils.readString(in);
		this.partition = WritableUtils.readVInt(in);
	}

}
