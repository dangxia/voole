/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class PartitionState implements Serializable, Writable {
	private BrokerAndTopicPartition brokerAndTopicPartition;
	private long latestOffset;
	private long earliestOffset;
	private long offset;

	public PartitionState() {
	}

	public PartitionState(BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
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

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public BrokerAndTopicPartition getBrokerAndTopicPartition() {
		return brokerAndTopicPartition;
	}

	public void setBrokerAndTopicPartition(
			BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof PartitionState) {
			PartitionState that = (PartitionState) obj;
			return Objects.equal(this.brokerAndTopicPartition, that.brokerAndTopicPartition)
					&& Objects.equal(this.earliestOffset, that.earliestOffset)
					&& Objects.equal(this.latestOffset, that.latestOffset)
					&& Objects.equal(this.offset, that.offset);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("partition", brokerAndTopicPartition)
				.add("latestOffset", latestOffset)
				.add("earliestOffset", earliestOffset).add("offset", offset)
				.toString();
	}

	public long estimateDataSize() {
		return latestOffset - offset;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.brokerAndTopicPartition.write(out);
		WritableUtils.writeVLong(out, this.earliestOffset);
		WritableUtils.writeVLong(out, this.latestOffset);
		WritableUtils.writeVLong(out, this.offset);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.brokerAndTopicPartition == null) {
			this.brokerAndTopicPartition = new BrokerAndTopicPartition();
		}
		this.brokerAndTopicPartition.readFields(in);
		this.earliestOffset = WritableUtils.readVLong(in);
		this.latestOffset = WritableUtils.readVLong(in);
		this.offset = WritableUtils.readVLong(in);
	}

}
