/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMeta;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class AvroTopicTransformMeta<T extends AvroKafkaTransformer> extends
		KafkaTopicTransformMeta<SpecificRecordBase, T> {
	private Schema schema;

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T createTransformer() throws KafkaTransformException {
		if (getTransformerClass() == AvroCtypeKafkaTransformer.class) {
			return (T) new AvroCtypeKafkaTransformer(getSchema());
		}
		return null;
	}
}
