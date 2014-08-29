/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro.order.util;

import java.util.Map.Entry;

import org.apache.avro.Schema;

import com.voole.hobbit2.kafka.avro.AvroSchemas;
import com.voole.hobbit2.kafka.avro.AvroSchemas.AvroSchemaRegister;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class OrderSchemaRegister implements AvroSchemaRegister {

	@Override
	public void register(AvroSchemas avroSchemas) {
		for (Entry<String, Schema> entry : OrderTopicsUtils.topicToSchema
				.entrySet()) {
			avroSchemas.register(entry.getKey(), entry.getValue());
		}
	}

}
