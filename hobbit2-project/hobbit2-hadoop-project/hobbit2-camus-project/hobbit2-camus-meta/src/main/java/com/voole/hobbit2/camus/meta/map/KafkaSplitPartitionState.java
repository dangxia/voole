/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class KafkaSplitPartitionState implements Writable {
	private KafkaPartition partition;
	private long latestOffset;
	private long earliestOffset;
	private long offset;

	public KafkaSplitPartitionState() {
	}

	public void setPartition(KafkaPartition partition) {
		this.partition = partition;
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

	public KafkaPartition getPartition() {
		return partition;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writePartition(out);
		WritableUtils.writeVLong(out, latestOffset);
		WritableUtils.writeVLong(out, earliestOffset);
		WritableUtils.writeVLong(out, offset);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.partition = readPartition(in);
		this.latestOffset = WritableUtils.readVLong(in);
		this.earliestOffset = WritableUtils.readVLong(in);
		this.offset = WritableUtils.readVLong(in);
	}

	private void writePartition(DataOutput out) throws IOException {
		writeBroker(out, partition.getBroker());
		WritableUtils.writeString(out, partition.getTopic());
		WritableUtils.writeVInt(out, partition.getPartition());
	}

	private void writeBroker(DataOutput out, Broker broker) throws IOException {
		WritableUtils.writeString(out, broker.host());
		WritableUtils.writeVInt(out, broker.port());
		WritableUtils.writeVInt(out, broker.id());
	}

	private KafkaPartition readPartition(DataInput in) throws IOException {
		return new KafkaPartition(readBroker(in), WritableUtils.readString(in),
				WritableUtils.readVInt(in));
	}

	private Broker readBroker(DataInput in) throws IOException {
		return new Broker(WritableUtils.readString(in),
				WritableUtils.readVInt(in), WritableUtils.readVInt(in));
	}

	/**
	 * @return
	 */
	public long estimateDataSize() {
		return latestOffset - offset;
	}

}
