/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.HashMultiset;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusSplit extends InputSplit implements Writable {
	private static final String[] EMPTY_LOCATIONS = new String[] {};

	private List<KafkaSplitPartitionState> splits = new ArrayList<KafkaSplitPartitionState>();
	private long length = 0;
	private String[] locations = EMPTY_LOCATIONS;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splits.size());
		for (KafkaSplitPartitionState r : splits)
			r.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		HashMultiset<String> locations = HashMultiset.create();
		for (int i = 0; i < size; i++) {
			KafkaSplitPartitionState r = new KafkaSplitPartitionState();
			r.readFields(in);
			splits.add(r);
			length += r.estimateDataSize();
			locations.add(r.getPartition().getBroker().host());
		}
		if (locations.size() == 0) {
			this.locations = EMPTY_LOCATIONS;
		} else {
			this.locations = new String[] { topLocation(locations) };
		}

	}

	private static String topLocation(HashMultiset<String> locations) {
		int max = 0;
		String topLocation = null;
		for (String location : locations.elementSet()) {
			int curr = locations.count(location);
			if (curr > max) {
				topLocation = location;
			}
		}
		return topLocation;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return locations;
	}
}
