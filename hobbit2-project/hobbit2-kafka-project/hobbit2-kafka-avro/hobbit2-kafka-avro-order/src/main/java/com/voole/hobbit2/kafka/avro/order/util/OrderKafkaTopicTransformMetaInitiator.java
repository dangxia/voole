/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro.order.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;

import com.voole.hobbit2.kafka.avro.AvroCtypeKafkaTransformer;
import com.voole.hobbit2.kafka.avro.AvroTopicTransformMeta;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMeta;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMetaInitiator.DefaultKafkaTopicTransformMetaInitiator;

public class OrderKafkaTopicTransformMetaInitiator extends
		DefaultKafkaTopicTransformMetaInitiator {
	public OrderKafkaTopicTransformMetaInitiator() {
	}

	@Override
	protected Collection<? extends KafkaTopicTransformMeta<?, ?>> getTopicToTransformMetaMap() {
		List<AvroTopicTransformMeta<AvroCtypeKafkaTransformer>> metas = new ArrayList<AvroTopicTransformMeta<AvroCtypeKafkaTransformer>>();
		for (Entry<String, Schema> entry : OrderTopicsUtils.topicToSchema.entrySet()) {
			AvroTopicTransformMeta<AvroCtypeKafkaTransformer> meta = new AvroTopicTransformMeta<AvroCtypeKafkaTransformer>();
			meta.setTopic(entry.getKey());
			meta.setTransformerClass(AvroCtypeKafkaTransformer.class);
			meta.setSchema(entry.getValue());
			metas.add(meta);
		}
		return metas;
	}

}