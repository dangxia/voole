/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.base.Objects;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusInputSplit extends InputSplit implements Writable {
	private BrokerAndTopicPartition brokerAndTopicPartition;
	private long latestOffset;
	private long offset;

	public static List<InputSplit> createSplits(
			Map<Broker, List<PartitionState>> brokerToPartitionStates,
			int mapperNum, int minSize) {
		long total = 0;
		for (Entry<Broker, List<PartitionState>> entry : brokerToPartitionStates
				.entrySet()) {
			List<PartitionState> partitionStates = entry.getValue();
			for (PartitionState kafkaPartitionState : partitionStates) {
				total += kafkaPartitionState.estimateDataSize();
			}
		}
		long avgSize = total / mapperNum;
		long targetSize = Math.max(minSize, avgSize);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (Entry<Broker, List<PartitionState>> entry : brokerToPartitionStates
				.entrySet()) {
			splits.addAll(split(entry.getValue(), targetSize));
		}
		return splits;
	}

	public static List<CamusInputSplit> split(List<PartitionState> list,
			long size) {
		List<CamusInputSplit> splits = new ArrayList<CamusInputSplit>();
		for (PartitionState kafkaPartitionState : list) {
			long offset = kafkaPartitionState.getOffset();
			long latestOffset = kafkaPartitionState.getLatestOffset();
			while (latestOffset - offset >= 2 * size) {
				splits.add(newSplit(
						kafkaPartitionState.getBrokerAndTopicPartition(),
						offset, offset + size));
				offset += size;
			}
			if (latestOffset - offset > 1.1 * size) {
				long halfSize = (latestOffset - offset) / 2;
				splits.add(newSplit(
						kafkaPartitionState.getBrokerAndTopicPartition(),
						offset, offset + halfSize));
				offset += halfSize;
			}
			splits.add(newSplit(
					kafkaPartitionState.getBrokerAndTopicPartition(), offset,
					latestOffset));
		}
		return splits;
	}

	private static CamusInputSplit newSplit(
			BrokerAndTopicPartition brokerAndTopicPartition, long offset,
			long latestOffset) {
		CamusInputSplit split = new CamusInputSplit();
		split.setBrokerAndTopicPartition(brokerAndTopicPartition);
		split.setLatestOffset(latestOffset);
		split.setOffset(offset);
		return split;
	}

	public BrokerAndTopicPartition getBrokerAndTopicPartition() {
		return brokerAndTopicPartition;
	}

	public void setBrokerAndTopicPartition(
			BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
	}

	public long getLatestOffset() {
		return latestOffset;
	}

	public void setLatestOffset(long latestOffset) {
		this.latestOffset = latestOffset;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public CamusInputSplit() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.brokerAndTopicPartition.write(out);
		WritableUtils.writeVLong(out, latestOffset);
		WritableUtils.writeVLong(out, offset);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.brokerAndTopicPartition == null) {
			this.brokerAndTopicPartition = new BrokerAndTopicPartition();
		}
		this.brokerAndTopicPartition.readFields(in);
		this.latestOffset = WritableUtils.readVLong(in);
		this.offset = WritableUtils.readVLong(in);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("brokerAndTopicPartition", brokerAndTopicPartition)
				.add("latestOffset", latestOffset).add("offset", offset)
				.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof CamusInputSplit) {
			CamusInputSplit that = (CamusInputSplit) obj;
			return Objects.equal(this.brokerAndTopicPartition,
					that.brokerAndTopicPartition)
					&& Objects.equal(this.latestOffset, that.latestOffset)
					&& Objects.equal(this.offset, that.offset);
		}
		return false;
	}

	public long estimateDataSize() {
		return latestOffset - offset;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return latestOffset - offset;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] { brokerAndTopicPartition.getBroker().host() };
	}

}
