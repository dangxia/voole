/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.base.Preconditions;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class AvroSchemas {
	private static final Map<String, Class<? extends SpecificRecordBase>> cache = new HashMap<String, Class<? extends SpecificRecordBase>>();

	public static Class<? extends SpecificRecordBase> getSchemaClass(
			Schema schema) throws ClassNotFoundException {
		Preconditions.checkNotNull(schema);
		String fullName = schema.getFullName();
		if (!cache.containsKey(fullName)) {
			generate(fullName);
		}
		return cache.get(fullName);

	}

	@SuppressWarnings("unchecked")
	private static synchronized void generate(String fullName)
			throws ClassNotFoundException {
		if (!cache.containsKey(fullName)) {
			cache.put(fullName, ((Class<? extends SpecificRecordBase>) Class
					.forName(fullName)));
		}
	}
}
