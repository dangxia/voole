package com.voole.hobbit2.storm.test.serializable;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Objects;
import com.voole.hobbit2.storm.test.serializable.MyDefaultKryoFactory.MyKryoSerializable;

public class TestKryoSerializableTopology {

	

	public static class DataModel implements MyKryoSerializable {

		private Integer num;
		private String name;

		public DataModel() {
		}

		public DataModel(Integer num) {
			this.num = num;
			this.name = "name_" + String.valueOf(num);
		}

		@Override
		public void write(Kryo kryo, Output output) {
			System.out.println("----------------------------");
			kryo.writeObjectOrNull(output, num, Integer.class);
			output.writeString(name);
		}

		@Override
		public void read(Kryo kryo, Input input) {
			setNum(kryo.readObjectOrNull(input, Integer.class));
			setName(input.readString());
		}

		public Integer getNum() {
			return num;
		}

		public void setNum(Integer num) {
			this.num = num;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("num", getNum())
					.add("name", getName()).toString();
		}

	}

	public static class Spout extends BaseRichSpout {
		private Random r;
		private SpoutOutputCollector _collector;

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
			r = new Random();
		}

		@Override
		public void nextTuple() {
			int num = r.nextInt(1000);
			DataModel data = null;
			if (num > 700) {
				data = new DataModel(null);
			} else {
				data = new DataModel(num);
			}
			_collector.emit(new Values(data));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("data"));

		}

	}

	public static class Bolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub

		}

		@Override
		public void execute(Tuple input) {
			System.out.println(input.getValue(0));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws InterruptedException {
		int maxSeconds = 15;
		int messageTimeOut = maxSeconds + 10;

		Config config = new Config();

		// config.registerSerialization(DataModel.class);
//		config.registerSerialization(MyKryoSerializable.class);
		config.setKryoFactory(MyDefaultKryoFactory.class);

		config.setMaxSpoutPending(5);
		config.setMessageTimeoutSecs(messageTimeOut);
		config.setMaxTaskParallelism(1);
		config.setNumWorkers(4);
		config.setNumAckers(1);
		config.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("num_spout", new Spout(), 2);
		builder.setBolt("num_bolt", new Bolt(), 2).shuffleGrouping("num_spout");

		LocalCluster local = new LocalCluster();
		local.submitTopology("test-metrics", config, builder.createTopology());

		TimeUnit.SECONDS.sleep(100);
		local.killTopology("test-metrics");
		local.shutdown();
	}

}
