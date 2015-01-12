package com.voole.hobbit2.storm.test.stream;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StreamTuples {
	public static enum SpoutType {
		STREAM_FIRST, STREAM_SECOND, STREAM_STATE_1;
	}

	public static final Fields FIRST_OUT_FIELDS = new Fields("type", "num");

	public static final Fields SECOND_OUT_FIELDS = new Fields("type", "name",
			"num");

	public static Values getFirstValues(long l) {
		return new Values(SpoutType.STREAM_FIRST, l);
	}

	public static Values getSecondValues(long l) {
		return new Values(SpoutType.STREAM_SECOND, "name_" + l, l);
	}

	public static class FirstTestStreamSpecial implements TestStreamSpecial {

		@Override
		public Values getValues() {
			return getFirstValues(1l);
		}

		@Override
		public Fields getOutFields() {
			return FIRST_OUT_FIELDS;
		}

		@Override
		public List<TestStreamSpoutPartition> getOrderedPartitions(
				Long allPartitionInfo) {
			List<TestStreamSpoutPartition> list = new ArrayList<TestStreamSpoutPartition>();
			for (int i = 0; i < allPartitionInfo; i++) {
				list.add(new TestStreamSpoutPartition("first_spout_", (long) i));
			}
			return list;
		}

		@Override
		public Long getPartitions() {
			return 1l;
		}

	}

	public static class SecondTestStreamSpecial implements TestStreamSpecial {

		@Override
		public Values getValues() {
			return getSecondValues(3l);
		}

		@Override
		public Fields getOutFields() {
			return SECOND_OUT_FIELDS;
		}

		@Override
		public List<TestStreamSpoutPartition> getOrderedPartitions(
				Long allPartitionInfo) {
			List<TestStreamSpoutPartition> list = new ArrayList<TestStreamSpoutPartition>();
			for (int i = 0; i < allPartitionInfo; i++) {
				list.add(new TestStreamSpoutPartition("second_spout_", (long) i));
			}
			return list;
		}

		@Override
		public Long getPartitions() {
			return 3l;
		}

	}

	public static class ThirdTestStreamSpecial implements TestStreamSpecial {

		@Override
		public Values getValues() {
			return getSecondValues(5l);
		}

		@Override
		public Fields getOutFields() {
			return SECOND_OUT_FIELDS;
		}

		@Override
		public List<TestStreamSpoutPartition> getOrderedPartitions(
				Long allPartitionInfo) {
			List<TestStreamSpoutPartition> list = new ArrayList<TestStreamSpoutPartition>();
			for (int i = 0; i < allPartitionInfo; i++) {
				list.add(new TestStreamSpoutPartition("third_spout_", (long) i));
			}
			return list;
		}

		@Override
		public Long getPartitions() {
			return 5l;
		}

	}
}
