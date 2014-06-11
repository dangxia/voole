/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.cache.RefreshCmdSender;
import com.voole.hobbit.cache.AbstractRefreshState.StateRefreshQueryFunction;
import com.voole.hobbit.cache.entity.OemInfo;
import com.voole.hobbit.cache.state.OemInfoCacheState;
import com.voole.hobbit.cache.state.OemInfoCacheState.OemInfoCacheStateFactory;
import com.voole.hobbit.storm.order.module.extra.OrderPlayBgnExtra;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class TestOrderTopology {
	private static Logger log = LoggerFactory
			.getLogger(TestOrderTopology.class);

	private static TransformerConfig localTransformerConfig = new TransformerConfig() {

		@Override
		public int getNumWorkers() {
			return 1;
		}

		@Override
		public Integer getMaxTaskParallelism() {
			return 5;
		}

		@Override
		public int getMaxSpoutPending() {
			return 2;
		}

		@Override
		public Integer getFetchSizeBytes() {
			return 1000;
		}

	};

	public interface TransformerConfig {
		int getMaxSpoutPending();

		int getNumWorkers();

		Integer getMaxTaskParallelism();

		Integer getFetchSizeBytes();
	}

	public static Config getConfig(TransformerConfig transformerConfig) {
		Config conf = new Config();
		conf.setMaxSpoutPending(transformerConfig.getMaxSpoutPending());
		conf.setNumWorkers(transformerConfig.getNumWorkers());
		if (transformerConfig.getMaxTaskParallelism() != null) {
			conf.setMaxTaskParallelism(transformerConfig
					.getMaxTaskParallelism());
		}

		return conf;
	}

	/**
	 * @author XuehuiHe
	 * @date 2014年6月6日
	 */
	public static final class PrintUrlMap extends BaseFilter {

		Gson gson;

		public PrintUrlMap() {
		}

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
			super.prepare(conf, context);
			GsonBuilder builder = new GsonBuilder();
			builder.setPrettyPrinting();
			gson = builder.create();
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			// Map<String, String> map = (Map<String, String>) tuple.get(0);
			// StringBuffer info = new StringBuffer("{\r\n");
			// for (Entry<String, String> entry : map.entrySet()) {
			// info.append("\t" + entry.getKey() + "\t:\t" + entry.getValue()
			// + "\r\n");
			// }
			// info.append("}");
			log.info(gson.toJson(tuple.get(0)));
			return true;
		}
	}

	public static TridentTopology createTopology(
			TransformerConfig transformerConfig) {
		TridentTopology topology = new TridentTopology();
		TridentState oemInfoCacheState = topology.newStaticState(
				new OemInfoCacheStateFactory()).parallelismHint(2);
		OrderBgnTopology orderBgnTopology = new OrderBgnTopology();
		if (transformerConfig.getFetchSizeBytes() != null) {
			orderBgnTopology.getKafkaConfig().setFetchSizeBytes(
					transformerConfig.getFetchSizeBytes());
		}
		Stream orderBgnStream = orderBgnTopology.build(topology);
		orderBgnStream.shuffle().stateQuery(oemInfoCacheState,
				new Fields("extra"), new OemInfoQueryFunction(),
				new Fields("oeminfo"));
		// .each(new Fields("extra", "oeminfo"), new HybridOemInfo(),
		// new Fields());

		topology.newStream("refresh-all", new RefreshCmdSender())
				.broadcast()
				.stateQuery(oemInfoCacheState,
						StateRefreshQueryFunction.INPUT_FIELDS,
						new StateRefreshQueryFunction(),
						StateRefreshQueryFunction.OUTPUT_FIELDS);
		return topology;
	}

	public static class OemInfoQueryFunction extends
			BaseQueryFunction<OemInfoCacheState, OemInfo> {

		@Override
		public List<OemInfo> batchRetrieve(OemInfoCacheState state,
				List<TridentTuple> args) {
			List<OemInfo> list = new ArrayList<OemInfo>();
			for (TridentTuple tuple : args) {
				OrderPlayBgnExtra extra = (OrderPlayBgnExtra) tuple.get(0);
				list.add(state.getOemInfo(extra.getOemid().intValue()));
			}
			return list;
		}

		@Override
		public void execute(TridentTuple tuple, OemInfo result,
				TridentCollector collector) {
			collector.emit(new Values(result));
		}

	}

	public static class HybridOemInfo extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			OrderPlayBgnExtra extra = (OrderPlayBgnExtra) tuple.get(0);
			OemInfo oemInfo = (OemInfo) tuple.get(1);
			System.out.println("oemid:" + extra.getOemid() + "\tspid:"
					+ oemInfo.getSpid());
		}

	}

	public static void main(String[] args) {
		TridentTopology topology = createTopology(localTransformerConfig);
		Config conf = getConfig(localTransformerConfig);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());
	}
}
