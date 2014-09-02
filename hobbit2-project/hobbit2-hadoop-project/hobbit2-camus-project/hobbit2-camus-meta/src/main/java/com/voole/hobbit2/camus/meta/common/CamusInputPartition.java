/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.voole.hobbit2.camus.meta.mapreduce.CamusInputSplit;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月2日
 */
public class CamusInputPartition implements Writable,
		Comparable<CamusInputPartition> {
	private TopicPartition topicPartition;
	private long startOffset;
	private long latestOffset;

	public CamusInputPartition() {
	}

	public CamusInputPartition(CamusInputPartition other) {
		this.topicPartition = other.topicPartition;
		this.startOffset = other.startOffset;
		this.latestOffset = other.latestOffset;
	}

	public CamusInputPartition(CamusInputSplit inputSplit) {
		this.topicPartition = inputSplit.getBrokerAndTopicPartition()
				.getPartition();
		this.startOffset = inputSplit.getOffset();
		this.latestOffset = inputSplit.getLatestOffset();
	}

	public TopicPartition getTopicPartition() {
		return topicPartition;
	}

	public void setTopicPartition(TopicPartition topicPartition) {
		this.topicPartition = topicPartition;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public long getLatestOffset() {
		return latestOffset;
	}

	public void setLatestOffset(long latestOffset) {
		this.latestOffset = latestOffset;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong(out, this.startOffset);
		WritableUtils.writeVLong(out, this.latestOffset);
		this.topicPartition.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.startOffset = WritableUtils.readVLong(in);
		this.latestOffset = WritableUtils.readVLong(in);
		if (this.topicPartition == null) {
			this.topicPartition = new TopicPartition();
		}
		this.topicPartition.readFields(in);
	}

	@Override
	public int compareTo(CamusInputPartition o) {
		return ComparisonChain.start()
				.compare(this.topicPartition, o.topicPartition)
				.compare(this.startOffset, o.startOffset)
				.compare(this.latestOffset, o.latestOffset).result();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(topicPartition, startOffset, latestOffset);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj != null && obj instanceof CamusInputPartition) {
			return compareTo((CamusInputPartition) obj) == 0;
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("topicPartition", topicPartition)
				.add("startOffset", startOffset)
				.add("latestOffset", latestOffset).toString();
	}

}
