/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.storm.order.topology.OrderTopology;
import com.voole.hobbit.storm.serializer.ProtoBuffKryoFactory;

/**
 * @author XuehuiHe
 * @date 2014年6月27日
 */
public class Run {
	private static Logger logger = LoggerFactory.getLogger(Run.class);

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		Config conf = new Config();
		conf.setMaxSpoutPending(10);
		conf.setNumWorkers(10);
		conf.setKryoFactory(ProtoBuffKryoFactory.class);
		// conf.setMaxTaskParallelism(1);

		TridentTopology topology = new TridentTopology();
		OrderTopology orderBgnTopology = new OrderTopology();
		orderBgnTopology.build(topology);

		StormSubmitter.submitTopology(args[0], conf, topology.build());

	}

	public static class Print extends BaseFilter {
		private Gson gson;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
			GsonBuilder gb = new GsonBuilder();
			gb.setPrettyPrinting();
			gson = gb.create();
			super.prepare(conf, context);
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			logger.info(gson.toJson(tuple));
			return true;
		}

	}
}
