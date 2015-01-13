package com.voole.hobbit2.camus.v3a;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.voole.hobbit2.hive.order.avro.V3aLogVersion1;

public class V3aAvroTopicUtils {
	protected static final BiMap<String, Schema> topicBiSchema;
	protected static final BiMap<String, Class<? extends SpecificRecordBase>> topicBiClazz;

	protected static final String TOPIC_V3A_LOG = "v3a_log";

	static {
		BiMap<String, Schema> topicToSchema = HashBiMap.create();
		BiMap<String, Class<? extends SpecificRecordBase>> topicToClazz = HashBiMap
				.create();

		add(topicToSchema, topicToClazz, TOPIC_V3A_LOG, V3aLogVersion1.class,
				V3aLogVersion1.getClassSchema());

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
