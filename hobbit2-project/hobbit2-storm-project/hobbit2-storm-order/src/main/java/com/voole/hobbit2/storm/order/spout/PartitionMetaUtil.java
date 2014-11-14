package com.voole.hobbit2.storm.order.spout;

import java.util.HashMap;

import org.json.simple.JSONObject;

public class PartitionMetaUtil extends HashMap<String, Object> {
	public static final String TOPOLOGY_NAME = "topology_name";
	public static final String PARTITION_OFFSET = "partition_offset";

	public static JSONObject newJSONObject(String topologyName,
			long partitionOffset) {
		JSONObject meta = new JSONObject();
		setTopologyName(meta, topologyName);
		setPartitionOffset(meta, partitionOffset);
		return meta;
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

		meta.remove(PARTITION_OFFSET);
		meta.remove(TOPOLOGY_NAME);
	}
}