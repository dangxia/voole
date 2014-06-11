/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.function;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.transformer.KafkaProtoBuffTransformer;
import com.voole.hobbit.transformer.KafkaProtoBuffTransformerFactory;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class KafkaTerminalProtoBuffTransformerFunction extends BaseFunction {
	private Map<String, KafkaProtoBuffTransformer<?>> transformerMap;
	private final static Logger logger = LoggerFactory
			.getLogger(KafkaTerminalProtoBuffTransformerFunction.class);

	public static final Fields INPUT_FIELDS = new Fields("topic", "offset",
			"partition", "bytes");
	public static final Fields OUTPUT_FIELDS = new Fields("origin");

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
		super.prepare(conf, context);
		transformerMap = new HashMap<String, KafkaProtoBuffTransformer<?>>();
		try {
			for (String topic : TopicProtoClassUtils.ORDER_TOPICS) {
				transformerMap.put(topic,
						KafkaProtoBuffTransformerFactory.getTransformer(topic));
			}
		} catch (Exception e) {
			logger.error("KafkaTerminalProtoBuffTransformerFunction init fail",
					e);
		}
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		byte[] bytes = tuple.getBinaryByField("bytes");
		long offset = tuple.getLongByField("offset");
		String topic = tuple.getStringByField("topic");
		try {
			if (offset % 10000 == 0) {
				int partition = tuple.getIntegerByField("partition");
				logger.info("topic:" + topic + "\tpartition:" + partition
						+ "\toffset:" + offset);
			}
			Object t = transformerMap.get(topic).transform(bytes);
			collector.emit(new Values(t));
		} catch (RuntimeException e) {
			logger.warn("transformer error:topic\t" + topic + "\tmsg\t"
					+ e.getMessage());
		} catch (Exception e) {
			logger.warn("transformer error", e);
		}
	}

}
