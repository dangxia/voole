/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import storm.trident.Stream;
import storm.trident.TridentTopology;

import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.storm.order.function.OrderPlayBgnExtraFunction;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class OrderBgnTopology extends OrderTopology {

	// private static Logger log =
	// LoggerFactory.getLogger(OrderBgnTopology.class);

	public OrderBgnTopology() {
		super("order-bgn-kafka-stream", TopicProtoClassUtils.ORDER_BGN_V2,
				TopicProtoClassUtils.ORDER_BGN_V3);
	}

	@Override
	public Stream build(TridentTopology topology) {
		Stream stream = super.build(topology);
		return stream.each(OrderPlayBgnExtraFunction.INPUT_FIELDS,
				new OrderPlayBgnExtraFunction(),
				OrderPlayBgnExtraFunction.OUTPUT_FIELDS);
	}
}
