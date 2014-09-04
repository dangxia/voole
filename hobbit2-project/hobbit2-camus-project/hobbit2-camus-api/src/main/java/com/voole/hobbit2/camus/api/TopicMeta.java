/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.transform.ITransformer;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class TopicMeta {
	private final String topic;
	private final Class<?> clazz;
	private final IStampFinder stampFinder;
	private final ITransformer<byte[], SpecificRecordBase> transformer;
	private final Schema schema;

	public TopicMeta(String topic, Class<?> clazz,
			IStampFinder stampFinder,
			ITransformer<byte[], SpecificRecordBase> transformer,
			Schema schema) {
		this.topic = topic;
		this.clazz = clazz;
		this.schema = schema;
		this.transformer = transformer;
		this.stampFinder = stampFinder;
	}

	public String getTopic() {
		return topic;
	}

	public Class<?> getClazz() {
		return clazz;
	}

	public IStampFinder getStampFinder() {
		return stampFinder;
	}

	public ITransformer<byte[], SpecificRecordBase> getTransformer() {
		return transformer;
	}

	public Schema getSchema() {
		return schema;
	}

}
