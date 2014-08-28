/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.common;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public class HourPartitioner implements
		Partitioner<CamusMapperTimeKeyAvro, SpecificRecordBase> {

	@Override
	public void partition(CamusMapperTimeKeyAvro p, CamusKafkaKey kafkakey,
			SpecificRecordBase value) {
		p.setTopic(kafkakey.getPartition().getTopic());
	}

}
