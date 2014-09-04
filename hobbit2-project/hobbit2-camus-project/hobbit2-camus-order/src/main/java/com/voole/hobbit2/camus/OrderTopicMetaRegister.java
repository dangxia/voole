/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.TopicMetaRegister;
import com.voole.hobbit2.camus.api.transform.AvroCtypeKafkaTransformer;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.camus.api.transform.ITransformer;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class OrderTopicMetaRegister implements TopicMetaRegister {

	@Override
	public List<TopicMeta> getTopicMetas() throws TransformException {
		List<TopicMeta> list = new ArrayList<TopicMeta>();
		IStampFinder stampFinder = new OrderStampFinder();
		for (String topic : OrderTopicsUtils.topicBiClazz.keySet()) {
			Class<?> clazz = OrderTopicsUtils.topicBiClazz.get(topic);
			Schema schema = OrderTopicsUtils.topicBiSchema.get(topic);
			ITransformer<byte[], SpecificRecordBase> transformer = new AvroCtypeKafkaTransformer(
					schema);
			list.add(new TopicMeta(topic, clazz, stampFinder, transformer,
					schema));
		}

		return list;
	}

}
