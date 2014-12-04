package com.voole.hobbit2.camus.direct;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.voole.hobbit2.hive.order.avro.BsPvPlayDryInfo;
import com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo;

public class DirectAvroTopicUtils {
	protected static final BiMap<String, Schema> topicBiSchema;
	protected static final BiMap<String, Class<? extends SpecificRecordBase>> topicBiClazz;

	protected static final String TOPIC_EPG_BS_PV = "epg_bs_pvinfo";
	protected static final String TOPIC_EPG_BS_REVENUE = "epg_bs_revenueinfo";

	static {
		BiMap<String, Schema> topicToSchema = HashBiMap.create();
		BiMap<String, Class<? extends SpecificRecordBase>> topicToClazz = HashBiMap
				.create();

		add(topicToSchema, topicToClazz, TOPIC_EPG_BS_PV,
				BsPvPlayDryInfo.class, BsPvPlayDryInfo.getClassSchema());

		add(topicToSchema, topicToClazz, TOPIC_EPG_BS_REVENUE,
				BsRevenueDryInfo.class, BsRevenueDryInfo.getClassSchema());

		topicBiSchema = ImmutableBiMap.copyOf(topicToSchema);
		topicBiClazz = ImmutableBiMap.copyOf(topicToClazz);
	}

	private static void add(BiMap<String, Schema> topicToSchema,
			BiMap<String, Class<? extends SpecificRecordBase>> topicToClazz,
			String topic, Class<? extends SpecificRecordBase> clazz,
			Schema schema) {
		topicToSchema.put(topic, schema);
		topicToClazz.put(topic, clazz);
	}

	public static boolean containsTopic(String topic) {
		return topicBiSchema.keySet().contains(topic);
	}
}
