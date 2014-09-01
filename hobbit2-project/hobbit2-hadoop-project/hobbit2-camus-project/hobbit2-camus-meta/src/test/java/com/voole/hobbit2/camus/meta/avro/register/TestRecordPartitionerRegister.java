/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro.register;

import com.voole.hobbit2.camus.meta.avro.TestRecordPartitioner;
import com.voole.hobbit2.kafka.common.partition.PartitionerRegister;
import com.voole.hobbit2.kafka.common.partition.Partitioners;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordPartitionerRegister implements PartitionerRegister {

	@Override
	public void register(Partitioners partitioners) {
		partitioners.register("test_record", new TestRecordPartitioner());
	}

}
