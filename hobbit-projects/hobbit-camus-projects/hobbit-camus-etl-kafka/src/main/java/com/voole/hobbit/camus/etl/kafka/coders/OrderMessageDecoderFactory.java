/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka.coders;

import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.coders.MessageDecoder;
import com.voole.hobbit.camus.coders.MessageDecoderException;
import com.voole.hobbit.transformer.KafkaTransformer;
import com.voole.hobbit.transformer.KafkaTransformerFactory;

/**
 * @author XuehuiHe
 * @date 2014年7月16日
 */
public class OrderMessageDecoderFactory implements MessageDecoderFactory {
	private final Map<String, MessageDecoder<?, ?>> cache;

	public OrderMessageDecoderFactory() {
		cache = new HashMap<String, MessageDecoder<?, ?>>();
	}

	@Override
	public MessageDecoder<?, ?> createMessageDecoder(JobContext context,
			String topicName) {
		if (!cache.containsKey(topicName)) {
			cache.put(
					topicName,
					new AvroMessageDecoder(KafkaTransformerFactory
							.getTransformer(topicName), topicName));
		}
		return cache.get(topicName);
	}

	public static class AvroMessageDecoder extends
			MessageDecoder<byte[], SpecificRecordBase> {

		private final KafkaTransformer transformer;
		private final String topic;

		public AvroMessageDecoder(KafkaTransformer transformer, String topic) {
			this.transformer = transformer;
			this.topic = topic;
		}

		@Override
		public CamusWrapper<SpecificRecordBase> decode(byte[] message) {
			CamusWrapper<SpecificRecordBase> wrapper;
			try {
				SpecificRecordBase record = transformer.transform(message);
				wrapper = new CamusWrapper<SpecificRecordBase>(record,
						getStamp(record));
			} catch (TransformerException e) {
				throw new MessageDecoderException(e);
			}
			return wrapper;
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

}
