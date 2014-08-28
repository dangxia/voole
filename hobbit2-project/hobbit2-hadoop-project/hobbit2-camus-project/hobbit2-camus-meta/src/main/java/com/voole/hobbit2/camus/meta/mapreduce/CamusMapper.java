/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.common.KafkaTransformer;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusMapper
		extends
		Mapper<CamusKafkaKey, BytesWritable, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
	private KafkaTransformer<SpecificRecordBase> transformer;

	private AvroKey<CamusMapperTimeKeyAvro> _key;
	private CamusMapperTimeKeyAvro avrokey;
	private AvroValue<SpecificRecordBase> _value;
	private String topic;

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		topic = findTopic(context);
		transformer = (KafkaTransformer<SpecificRecordBase>) CamusMetaConfigs
				.getTopicTransformMetas(context).getTransformer(topic);
		avrokey = new CamusMapperTimeKeyAvro();
		avrokey.setTopic(topic);
		_key = new AvroKey<CamusMapperTimeKeyAvro>(avrokey);
		_value = new AvroValue<SpecificRecordBase>();
	}

	protected String findTopic(Context context) {
		return ((CamusInputSplit) context.getInputSplit())
				.getBrokerAndTopicPartition().getPartition().getTopic();
	}

	@Override
	protected void map(CamusKafkaKey key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		try {
			SpecificRecordBase v = transformer.transform(value.getBytes());
			long stamp = getStamp(v);
			avrokey.setCategoryTime(stamp / 24 * 60 * 60 * 1000 * 24 * 60 * 60
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
