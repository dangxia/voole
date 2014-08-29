/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.meta;

import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public abstract class TopicTransformMeta<M, N extends KafkaTransformer<M>> {
	private String topic;
	private Class<N> transformerClass;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Class<N> getTransformerClass() {
		return transformerClass;
	}

	public void setTransformerClass(Class<N> transformerClass) {
		this.transformerClass = transformerClass;
	}

	public abstract N createTransformer() throws KafkaTransformException;

}
