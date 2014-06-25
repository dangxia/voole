/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import kafka.api.OffsetRequest;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.storm.order.filter.TickFilter;
import com.voole.hobbit.storm.order.function.aggregator.OnlineUserModifierCombiner;
import com.voole.hobbit.storm.order.function.aggregator.PlayExtraCombinerAggregator;
import com.voole.hobbit.storm.order.function.aggregator.SessionTickCombinerAggregator;
import com.voole.hobbit.storm.order.function.extra.PlayExtraFunction;
import com.voole.hobbit.storm.order.function.transformer.TransformerFunction;
import com.voole.hobbit.storm.order.state.OrderExtraInfoQueryState.OrderExtraInfoQuery;
import com.voole.hobbit.storm.order.state.OrderExtraInfoQueryState.OrderExtraInfoQueryStateFactory;
import com.voole.hobbit.storm.order.state.PlayExtraStateImpl.PlayExtraUpdater;
import com.voole.hobbit.storm.order.state.updater.HidTickStateUpdater;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class OrderTopology2 {
	private final ZkHosts hosts;
	private final KafkaConfig kafkaConfig;
	private final String streamTxid;

	public OrderTopology2() {
		this.streamTxid = "order-kafka-stream";
		hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		kafkaConfig = new KafkaConfig(hosts,
				TopicProtoClassUtils.ORDER_TOPICS.toArray(new String[] {}));
		kafkaConfig.forceStartOffsetTime(OffsetRequest.LatestTime());
	}

	public ZkHosts getHosts() {
		return hosts;
	}

	public KafkaConfig getKafkaConfig() {
		return kafkaConfig;
	}

	public String getStreamTxid() {
		return streamTxid;
	}

	public Stream build(TridentTopology topology) {
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout(
				getKafkaConfig());
		TridentState extraInfoState = topology
				.newStaticState(new OrderExtraInfoQueryStateFactory());
		Stream stream = topology.newStream(getStreamTxid(), orderKafkaSpout)
				.parallelismHint(getKafkaConfig().getTopics().length * 4)
				.shuffle();
		stream = TransformerFunction.each(stream, getKafkaConfig().getTopics());
		stream = TickFilter.filte(stream);
		stream = PlayExtraFunction.each(stream);
		stream = PlayExtraCombinerAggregator.group(stream);
		stream = OrderExtraInfoQuery.query(stream, extraInfoState);
		TridentState playExtraState = PlayExtraUpdater.partitionPersist(stream);
		stream = playExtraState.newValuesStream();
		stream = SessionTickCombinerAggregator.group(stream);
		TridentState hidTickState = HidTickStateUpdater
				.partitionPersist(stream);
		stream = hidTickState.newValuesStream();
		stream = OnlineUserModifierCombiner.combine(stream).newValuesStream();
		return stream;
	}
}
