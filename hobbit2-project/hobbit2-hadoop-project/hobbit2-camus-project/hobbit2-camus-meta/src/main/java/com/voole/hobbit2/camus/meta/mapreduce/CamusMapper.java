/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.Mapper;

import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKey;
import com.voole.hobbit2.kafka.avro.order.util.OrderTopicsUtils;
import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.KafkaTransformerFactory;
import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusMapper
		extends
		Mapper<CamusKafkaKey, byte[], CamusMapperTimeKey, AvroValue<SpecificRecordBase>> {
	private KafkaTransformer<SpecificRecordBase> transformer;
	private String topic;

	private CamusMapperTimeKey _key;
	private AvroValue<SpecificRecordBase> _value;

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		OrderTopicsUtils.registerTransformMetas();
		CamusInputSplit split = (CamusInputSplit) context.getInputSplit();
		this.topic = split.getBrokerAndTopicPartition().getPartition()
				.getTopic();
		try {
			transformer = (KafkaTransformer<SpecificRecordBase>) KafkaTransformerFactory
					.getTransformer(topic);
		} catch (KafkaTransformException e) {
			throw new RuntimeException(e);
		}

		_key = new CamusMapperTimeKey();
		_key.setTopic(topic);
		_value = new AvroValue<SpecificRecordBase>();
	}

	@Override
	protected void map(CamusKafkaKey key, byte[] value, Context context)
			throws IOException, InterruptedException {
		try {
			SpecificRecordBase v = transformer.transform(value);
			long stamp = getStamp(v);
			_key.setCategoryTime(stamp / 24 * 60 * 60 * 1000 * 24 * 60 * 60
					* 1000);
			_value.datum(v);
			context.write(_key, _value);
		} catch (Exception e) {
			e.printStackTrace();
			// TODO
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}

	private long getStamp(SpecificRecordBase record) {
		long stamp = 0;
		if (topic.equals("t_playbgn_v2") || topic.equals("t_playbgn_v3")) {
			stamp = (Long) record.get("playTick");
		} else if (topic.equals("t_playalive_v2")
				|| topic.equals("t_playalive_v3")) {
			stamp = (Long) record.get("aliveTick");
		} else {
			stamp = (Long) record.get("endTick");
		}
		return stamp * 1000;
	}
}
