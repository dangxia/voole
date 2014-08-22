/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.kafka.common.KafkaTransformer;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public interface AvroKafkaTransformer extends
		KafkaTransformer<SpecificRecordBase> {

}
