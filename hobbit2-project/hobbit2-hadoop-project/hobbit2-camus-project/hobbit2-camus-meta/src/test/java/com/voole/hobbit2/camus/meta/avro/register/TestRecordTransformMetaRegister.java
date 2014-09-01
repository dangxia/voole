/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro.register;

import com.voole.hobbit2.camus.meta.avro.TestRecord;
import com.voole.hobbit2.camus.meta.avro.TestRecordTransformer;
import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;
import com.voole.hobbit2.kafka.common.meta.TopicTransformMeta;
import com.voole.hobbit2.kafka.common.meta.TopicTransformMetaRegister;
import com.voole.hobbit2.kafka.common.meta.TopicTransformMetas;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordTransformMetaRegister implements
		TopicTransformMetaRegister {

	@Override
	public void register(TopicTransformMetas topicTransformMetas) {

		TopicTransformMeta<TestRecord, TestRecordTransformer> meta = new TopicTransformMeta<TestRecord, TestRecordTransformer>() {
			@Override
			public TestRecordTransformer createTransformer()
					throws KafkaTransformException {
				return new TestRecordTransformer();
			}
		};

		meta.setTopic("test_record");
		meta.setTransformerClass(TestRecordTransformer.class);
		topicTransformMetas.register(meta);
	}

}
