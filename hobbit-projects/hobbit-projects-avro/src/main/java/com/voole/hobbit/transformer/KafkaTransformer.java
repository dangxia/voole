/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import javax.xml.transform.TransformerException;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * @author XuehuiHe
 * @date 2014年5月30日
 */
public interface KafkaTransformer {
	SpecificRecordBase transform(byte[] bytes) throws TransformerException;
}
