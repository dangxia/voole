/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.base.Objects;
import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.config.props.KafkaConfigKeys;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.ZookeeperUtils;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartition;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartitionState;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusInputSplit extends InputSplit implements Writable {
	private KafkaPartition partition;
	private long latestOffset;
	private long offset;

	public static void main(String[] args) {
		Hobbit2Configuration conf = new Hobbit2Configuration();
		System.out.println(conf
				.getString(KafkaConfigKeys.KAFKA_ZOOKEEPER_CONNECT));
		ZkClient client = ZookeeperUtils.createZKClient(
				conf.getString(KafkaConfigKeys.KAFKA_ZOOKEEPER_CONNECT),
				conf.getInt(KafkaConfigKeys.KAFKA_TIME_OUT_MS),
				conf.getInt(KafkaConfigKeys.KAFKA_TIME_OUT_MS));
		Map<Broker, List<KafkaPartitionState>> map = KafkaUtils
				.getPartitionState(client, "t_playbgn_v2", "t_playbgn_v3");
		List<InputSplit> splits = createSplits(map, 20, 10000);
		for (InputSplit inputSplit : splits) {
			System.out.println(inputSplit);
		}
		client.close();
	}

	public static List<InputSplit> createSplits(
			Map<Broker, List<KafkaPartitionState>> brokerToPartitionStates,
			int mapperNum, int minSize) {
		long total = 0;
		for (Entry<Broker, List<KafkaPartitionState>> entry : brokerToPartitionStates
				.entrySet()) {
			List<KafkaPartitionState> partitionStates = entry.getValue();
			for (KafkaPartitionState kafkaPartitionState : partitionStates) {
				total += kafkaPartitionState.estimateDataSize();
			}
		}
		long avgSize = total / mapperNum;
		long targetSize = Math.max(minSize, avgSize);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (Entry<Broker, List<KafkaPartitionState>> entry : brokerToPartitionStates
				.entrySet()) {
			splits.addAll(split(entry.getValue(), targetSize));
		}
		return splits;
	}

	public static List<CamusInputSplit> split(List<KafkaPartitionState> list,
			long size) {
		List<CamusInputSplit> splits = new ArrayList<CamusInputSplit>();
		for (KafkaPartitionState kafkaPartitionState : list) {
			long offset = kafkaPartitionState.getOffset();
			long latestOffset = kafkaPartitionState.getLatestOffset();
			while (latestOffset - offset >= 2 * size) {
				CamusInputSplit split = new CamusInputSplit();
				split.setPartition(kafkaPartitionState.getPartition());
				split.setLatestOffset(offset + size);
				split.setOffset(offset);
				splits.add(split);
				offset += size;
			}
			CamusInputSplit split = new CamusInputSplit();
			split.setPartition(kafkaPartitionState.getPartition());
			split.setLatestOffset(latestOffset);
			split.setOffset(offset);
			splits.add(split);
		}
		return splits;
	}

	public CamusInputSplit() {
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
		WritableUtils.writeVLong(out, offset);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.partition = readPartition(in);
		this.latestOffset = WritableUtils.readVLong(in);
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

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("partition", partition)
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
			return Objects.equal(this.partition, that.partition)
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
		return new String[] { partition.getBroker().host() };
	}

}
