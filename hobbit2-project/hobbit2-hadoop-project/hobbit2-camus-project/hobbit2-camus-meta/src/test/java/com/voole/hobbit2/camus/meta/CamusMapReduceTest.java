/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.camus.meta.mapreduce.CamusMapper;
import com.voole.hobbit2.camus.meta.mapreduce.CamusReducer;
import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;
import com.voole.hobbit2.kafka.avro.order.util.OrderTopicsUtils;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月28日
 */
public class CamusMapReduceTest {
	private MapDriver<CamusKafkaKey, BytesWritable, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> mapDriver;
	private ReduceDriver<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> reduceDriver;
	private MapReduceDriver<CamusKafkaKey, BytesWritable, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> mapReduceDriver;
	private static final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	@Before
	public void setUp() {
		CamusMapper mapper = new CamusMapper() {
			@Override
			protected String findTopic(Context context) {
				return OrderTopicsUtils.TOPIC_ORDER_BGN_V2;
			}
		};
		CamusReducer reducer = new CamusReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

		initConfigs(mapDriver.getConfiguration());
	}

	@Test
	public void testMapper() throws IOException {
		CamusKafkaKey key = new CamusKafkaKey();
		key.setPartition(new TopicPartition(
				OrderTopicsUtils.TOPIC_ORDER_BGN_V2, 2));
		String str = "145	0	30	1305905550	BC83A71935BE00000000000000000000	1000	1694542016	8546749285776193522	0	0	FF2D905B74EE3A7A5C8CBC352BC3FC20	1363914803	7	992688	220	vosp://cdn.voole.com:3528/play?fid=ff2d905b74ee3a7a5c8cbc352bc3fc20&keyid=0&stamp=1406184247&is3d=0&fm=7&tvid=BC83A71935BE&bit=1300&auth=6caba6f89099539475e09a81e9696066&ext=oid:433,eid:100105,code:ASTBOX_movie_index&s=1	1406184279	0	2874777552";
		byte[] val = str.getBytes();
		mapDriver.withInput(key, new BytesWritable(val));

		List<Pair<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>>> list = mapDriver
				.run();
		for (Pair<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> pair : list) {
			System.out.println(pair.getFirst().datum());
			System.out.println(pair.getSecond().datum());

			long t = pair.getFirst().datum().getCategoryTime();
			System.out.println(df.format(new Date(t)));
		}
		// mapDriver.withInput(new LongWritable(), new Text(
		// "655209;1;796764372490213;804422938115889;6"));
		// mapDriver.withOutput(new Text("6"), new IntWritable(1));
		// mapDriver.runTest();
		// mapDriver.
	}

	private boolean initConfigs(Configuration conf) {
		Hobbit2Configuration hobbit2Configuration = new Hobbit2Configuration();
		for (@SuppressWarnings("unchecked")
		Iterator<String> iterator = hobbit2Configuration.getKeys(); iterator
				.hasNext();) {
			String key = iterator.next();
			conf.set(key, hobbit2Configuration.getString(key));
		}
		conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		AvroSerialization.setKeyWriterSchema(conf,
				CamusMapperTimeKeyAvro.getClassSchema());
		AvroSerialization.setKeyReaderSchema(conf,
				CamusMapperTimeKeyAvro.getClassSchema());
		AvroSerialization.addToConfiguration(conf);
		Schema vaSchema = getMapValueSchema();
		AvroSerialization.setValueWriterSchema(conf, vaSchema);
		AvroSerialization.setValueReaderSchema(conf, vaSchema);
		AvroSerialization.addToConfiguration(conf);
		return true;
	}

	public static void main(String[] args) {

		System.out.println(df.format(new Date(1406184279000l
				/ (24 * 60 * 60 * 1000) * 24 * 60 * 60 * 1000)));
	}

	public static Schema getMapValueSchema() {

		List<Schema> schemas = new ArrayList<Schema>();
		schemas.add(OrderPlayBgnReqV2.getClassSchema());
		schemas.add(OrderPlayBgnReqV3.getClassSchema());

		schemas.add(OrderPlayAliveReqV2.getClassSchema());
		schemas.add(OrderPlayAliveReqV3.getClassSchema());

		schemas.add(OrderPlayEndReqV2.getClassSchema());
		schemas.add(OrderPlayEndReqV3.getClassSchema());

		return Schema.createUnion(schemas);
	}
}
