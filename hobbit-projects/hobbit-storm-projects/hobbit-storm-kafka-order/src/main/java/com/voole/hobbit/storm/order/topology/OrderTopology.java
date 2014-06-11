/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import kafka.api.OffsetRequest;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hobbit.storm.order.function.TickFilter;
import com.voole.hobbit.storm.order.function.TransformerFunction;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public abstract class OrderTopology {
	private final ZkHosts hosts;
	private final KafkaConfig kafkaConfig;
	private final String streamTxid;

	public OrderTopology(String streamTxid, String... topics) {
		this.streamTxid = streamTxid;
		hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		kafkaConfig = new KafkaConfig(hosts, topics);
		kafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());
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
		return topology
				.newStream(getStreamTxid(), orderKafkaSpout)
				.parallelismHint(getKafkaConfig().getTopics().length * 4)
				.shuffle()
				.each(TransformerFunction.INPUT_FIELDS,
						new TransformerFunction(getKafkaConfig().getTopics()),
						TransformerFunction.OUTPUT_FIELDS)
				.each(TransformerFunction.OUTPUT_FIELDS, new TickFilter())
				.project(TransformerFunction.OUTPUT_FIELDS);
	}
}
