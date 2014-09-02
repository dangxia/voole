/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.partitioner;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.avro.order.util.AbstractOrderPartitionerRegister.AbstractOrderPartitioner;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class OrderPartitioner extends
		AbstractOrderPartitioner<CamusMapperTimeKeyAvro, CamusKafkaKey> {
	private static SimpleDateFormat df = new SimpleDateFormat("/yyyy/MM/dd/HH/");

	@Override
	public void partition(CamusMapperTimeKeyAvro p, CamusKafkaKey kafkakey,
			SpecificRecordBase value) {
		p.setTopic(kafkakey.getPartition().getTopic());
		p.setCategoryTime(getCategoryTime(value));
	}

	private long getCategoryTime(SpecificRecordBase value) {
		long stamp = getStamp(value);
		stamp = stamp / (60 * 60 * 1000) * 60 * 60 * 1000;
		return stamp;
	}

	@Override
	public String getPath(CamusMapperTimeKeyAvro p) {
		return p.getTopic() + "/hourly"
				+ df.format(new Date(p.getCategoryTime()));
	}
}
