/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.transformer.OrderTopicsTransformer;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class TransformerFunction extends BaseFunction {

	public static final Fields INPUT_FIELDS = new Fields("topic", "offset",
			"partition", "bytes");
	public static final Fields OUTPUT_FIELDS = new Fields("proto");

	private final static Logger logger = LoggerFactory
			.getLogger(TransformerFunction.class);
	private final String[] topics;
	private OrderTopicsTransformer transformer;

	public TransformerFunction(String... topics) {
		this.topics = topics;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
		super.prepare(conf, context);
		transformer = new OrderTopicsTransformer(this.topics);

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		byte[] bytes = tuple.getBinaryByField("bytes");
		long offset = tuple.getLongByField("offset");
		String topic = tuple.getStringByField("topic");
		if (offset % 10000 == 0) {
			int partition = tuple.getIntegerByField("partition");
			logger.info("topic:" + topic + "\tpartition:" + partition
					+ "\toffset:" + offset);
		}
		Object t = transformer.transformer(topic, bytes);
		collector.emit(new Values(t));
	}

}
