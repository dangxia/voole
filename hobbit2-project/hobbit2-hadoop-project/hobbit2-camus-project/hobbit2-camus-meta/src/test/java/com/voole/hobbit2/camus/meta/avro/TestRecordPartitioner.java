/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro;

import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.common.partition.Partitioner;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordPartitioner implements
		Partitioner<CamusMapperTimeKeyAvro, CamusKafkaKey, TestRecord> {

	@Override
	public void partition(CamusMapperTimeKeyAvro p, CamusKafkaKey kafkakey,
			TestRecord value) {
		p.setTopic(kafkakey.getPartition().getTopic());
		p.setCategoryTime(getCategoryTime(value));
	}

	private long getCategoryTime(TestRecord value) {
		long stamp = value.getStamp();
		stamp = stamp / (24 * 60 * 60 * 1000) * 24 * 60 * 60 * 1000;
		stamp -= 8 * 60 * 60 * 1000;
		return stamp;
	}

}
