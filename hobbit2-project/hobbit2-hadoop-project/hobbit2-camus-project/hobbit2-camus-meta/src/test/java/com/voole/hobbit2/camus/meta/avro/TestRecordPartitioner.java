/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro;

import com.voole.hobbit2.camus.meta.common.CamusKey;
import com.voole.hobbit2.kafka.common.partition.Partitioner;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordPartitioner implements
		Partitioner<CamusKey, CamusKey, TestRecord> {

	@Override
	public void partition(CamusKey p, CamusKey kafkakey, TestRecord value) {
		p.setStamp(getCategoryTime(value));
	}

	private long getCategoryTime(TestRecord value) {
		long stamp = value.getStamp();
		stamp = stamp / (24 * 60 * 60 * 1000) * 24 * 60 * 60 * 1000;
		stamp -= 8 * 60 * 60 * 1000;
		return stamp;
	}

	@Override
	public String getPath(CamusKey p) {
		// TODO Auto-generated method stub
		return null;
	}

}
