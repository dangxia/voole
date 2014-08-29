/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class AvroSchemas {
	private final Map<String, Schema> topicToSchema;

	public AvroSchemas() {
		topicToSchema = new HashMap<String, Schema>();
	}

	public AvroSchemas(AvroSchemaRegister... registers) {
		this();
		for (AvroSchemaRegister register : registers) {
			register.register(this);
		}
	}

	public void register(String topic, Schema schema) {
		Preconditions.checkNotNull(schema, "schema is null");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
				"topic is empty");
		topicToSchema.put(topic, schema);
	}

	public void unregister(String topic) {
		topicToSchema.remove(topic);
	}

	public Schema getSchema(String topic) {
		if (topicToSchema.containsKey(topic)) {
			return topicToSchema.get(topic);
		} else {
			throw new RuntimeException("topic:" + topic + " Schema not found");
		}
	}

	public Collection<Schema> getAllSchema() {
		return topicToSchema.values();
	}

	public interface AvroSchemaRegister {
		public void register(AvroSchemas avroSchemas);
	}
}
