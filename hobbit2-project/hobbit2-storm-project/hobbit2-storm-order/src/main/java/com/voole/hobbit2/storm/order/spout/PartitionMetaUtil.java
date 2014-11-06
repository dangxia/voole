package com.voole.hobbit2.storm.order.spout;

import java.util.HashMap;

import org.json.simple.JSONObject;

import com.google.common.base.Optional;

public class PartitionMetaUtil extends HashMap<String, Object> {
	public static final String TOPOLOGY_NAME = "topology_name";
	public static final String PARTITION_OFFSET = "partition_offset";

	public static final String NOEND_INDEX = "noend_index";
	public static final String NOEND_OFFSET = "noend_offset";

	public static final long NOEND_INITING_OFFSET = 0l;

	public static JSONObject newJSONObject(String topologyName,
			long partitionOffset) {
		JSONObject meta = new JSONObject();
		setTopologyName(meta, topologyName);
		setPartitionOffset(meta, partitionOffset);
		return meta;
	}

	public static Optional<Integer> getNoendIndex(JSONObject meta) {
		Number noendIndex = (Number) meta.get(NOEND_INDEX);
		if (noendIndex == null || noendIndex.intValue() == -1) {
			return Optional.absent();
		}
		return Optional.of(noendIndex.intValue());
	}

	@SuppressWarnings("unchecked")
	public static void setNextNoendIndex(JSONObject meta, int nextNoendIndex) {
		meta.put(NOEND_INDEX, nextNoendIndex);
		meta.put(NOEND_OFFSET, NOEND_INITING_OFFSET);
	}

	public static void finishedNoend(JSONObject meta) {
		meta.remove(NOEND_INDEX);
		meta.remove(NOEND_OFFSET);
	}

	public static long getNoendOffset(JSONObject meta) {
		Number noendOffset = (Number) meta.get(NOEND_OFFSET);
		if (noendOffset == null) {
			return NOEND_INITING_OFFSET;
		}
		return noendOffset.longValue();
	}

	@SuppressWarnings("unchecked")
	public static void setNoendOffset(JSONObject meta, long noendOffset) {
		meta.put(NOEND_OFFSET, noendOffset);
	}

	@SuppressWarnings("unchecked")
	public static void setTopologyName(JSONObject meta, String topologyName) {
		meta.put(TOPOLOGY_NAME, topologyName);
	}

	public static String getTopologyName(JSONObject meta) {
		return (String) meta.get(TOPOLOGY_NAME);
	}

	@SuppressWarnings("unchecked")
	public static void setPartitionOffset(JSONObject meta, long offset) {
		meta.put(PARTITION_OFFSET, offset);
	}

	public static Long getPartitionOffset(JSONObject meta) {
		return (Long) meta.get(PARTITION_OFFSET);
	}

	public static void clean(JSONObject meta) {
		meta.remove(NOEND_INDEX);
		meta.remove(NOEND_OFFSET);

		meta.remove(PARTITION_OFFSET);
		meta.remove(TOPOLOGY_NAME);
	}
}