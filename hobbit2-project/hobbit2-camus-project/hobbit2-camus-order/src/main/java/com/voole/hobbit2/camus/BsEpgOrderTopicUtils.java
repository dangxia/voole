package com.voole.hobbit2.camus;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;

public class BsEpgOrderTopicUtils {
	protected static final BiMap<String, Schema> topicBiSchema;
	protected static final BiMap<String, Class<? extends SpecificRecordBase>> topicBiClazz;
	protected static final String TOPIC_EPG_BS_PLAYINFO = "epg_bs_playinfo";

	static {
		BiMap<String, Schema> topicToSchema = HashBiMap.create(6);
		BiMap<String, Class<? extends SpecificRecordBase>> topicToClazz = HashBiMap
				.create(6);

		add(topicToSchema, topicToClazz, TOPIC_EPG_BS_PLAYINFO,
				BsEpgPlayInfo.class, BsEpgPlayInfo.getClassSchema());

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
