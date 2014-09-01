/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro.register;

import com.voole.hobbit2.camus.meta.avro.TestRecord;
import com.voole.hobbit2.kafka.avro.AvroSchemas;
import com.voole.hobbit2.kafka.avro.AvroSchemas.AvroSchemaRegister;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordSchemaRegister implements AvroSchemaRegister {

	@Override
	public void register(AvroSchemas avroSchemas) {
		avroSchemas.register("test_record", TestRecord.getClassSchema());
	}
}
