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
import com.voole.hobbit.storm.order.function.query.OnlineUserQuery;
import com.voole.hobbit.storm.order.function.transformer.TransformerFunction;
import com.voole.hobbit.storm.order.state.OrderExtraInfoQueryState.OrderExtraInfoQuery;
import com.voole.hobbit.storm.order.state.OrderExtraInfoQueryState.OrderExtraInfoQueryStateFactory;
import com.voole.hobbit.storm.order.state.PlayExtraStateImpl.PlayExtraUpdater;
import com.voole.hobbit.storm.order.state.updater.HidTickStateUpdater;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class OrderTopology {
	private final ZkHosts hosts;
	private final KafkaConfig kafkaConfig;
	private final String streamTxid;
	private final int stormClusterNum;

	public OrderTopology() {
		this.streamTxid = "order-kafka-stream";
		hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		kafkaConfig = new KafkaConfig(hosts,
				TopicProtoClassUtils.ORDER_TOPICS.toArray(new String[] {}));
		kafkaConfig.forceStartOffsetTime(OffsetRequest.LatestTime());
		stormClusterNum = 4;
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
		TridentState extraInfoState = topology.newStaticState(
				new OrderExtraInfoQueryStateFactory()).parallelismHint(
				getStormClusterNum());
		Stream stream = topology.newStream(getStreamTxid(), orderKafkaSpout)
				.parallelismHint(
						getKafkaConfig().getTopics().length
								* getStormClusterNum());
		stream = TransformerFunction.each(stream, getKafkaConfig().getTopics());
		stream = TickFilter.filte(stream);
		stream = PlayExtraFunction.each(stream).shuffle();
		stream = PlayExtraCombinerAggregator.group(stream).parallelismHint(
				getStormClusterNum());
		stream = OrderExtraInfoQuery.query(stream, extraInfoState);
		TridentState playExtraState = PlayExtraUpdater.partitionPersist(stream)
				.parallelismHint(getStormClusterNum());
		stream = playExtraState.newValuesStream();
		stream = SessionTickCombinerAggregator.group(stream).parallelismHint(
				getStormClusterNum());
		TridentState hidTickState = HidTickStateUpdater
				.partitionPersist(stream).parallelismHint(getStormClusterNum());
		stream = hidTickState.newValuesStream();
		TridentState onlineUserState = OnlineUserModifierCombiner
				.combine(stream);
		stream = onlineUserState.newValuesStream();

		OnlineUserQuery.query(topology.newDRPCStream("query"), onlineUserState);

		return stream;
	}

	public int getStormClusterNum() {
		return stormClusterNum;
	}
}
