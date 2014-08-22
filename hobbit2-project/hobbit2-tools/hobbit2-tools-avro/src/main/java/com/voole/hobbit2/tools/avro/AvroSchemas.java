/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class AvroSchemas {
	@SuppressWarnings("unchecked")
	public static Class<? extends SpecificRecordBase> getSchemaClass(
			Schema schema) throws ClassNotFoundException {
		return ((Class<? extends SpecificRecordBase>) Class.forName(schema
				.getFullName()));

	}
}
