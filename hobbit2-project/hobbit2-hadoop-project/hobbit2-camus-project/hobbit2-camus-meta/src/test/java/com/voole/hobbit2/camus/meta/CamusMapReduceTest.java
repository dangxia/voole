/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import static com.voole.hobbit2.camus.meta.AvroJobTest.setMapOutputKeySchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setMapOutputValueSchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setOutputKeySchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setOutputValueSchema;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.camus.meta.mapreduce.CamusMapper;
import com.voole.hobbit2.camus.meta.mapreduce.CamusReducer;
import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.kafka.avro.AvroSchemas;
import com.voole.hobbit2.kafka.avro.AvroSchemas.AvroSchemaRegister;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
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

	private CamusKafkaKey inputKey;
	private BytesWritable inputValue;
	private CamusMapperTimeKeyAvro _outKey;
	private AvroKey<CamusMapperTimeKeyAvro> outKey;
	private AvroValue<SpecificRecordBase> outVal;

	@Before
	public void setUp() throws ParseException {
		CamusMapper mapper = new CamusMapper() {
			@Override
			protected String findTopic(Context context) {
				return OrderTopicsUtils.TOPIC_ORDER_BGN_V2;
			}

			@Override
			protected Path getWorkingDirectory(Context context)
					throws IOException {
				return new Path("/tmp");
			}
		};
		CamusReducer reducer = new CamusReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

		initConfigs(mapDriver.getConfiguration());
		initConfigs(reduceDriver.getConfiguration());
		initConfigs(mapReduceDriver.getConfiguration());

		inputKey = new CamusKafkaKey();
		inputKey.setPartition(new TopicPartition(
				OrderTopicsUtils.TOPIC_ORDER_BGN_V2, 2));
		String str = "1 145	0	30	1305905550	BC83A71935BE00000000000000000000	1000	1694542016	8546749285776193522	0	0	FF2D905B74EE3A7A5C8CBC352BC3FC20	1363914803	7	992688	220	vosp://cdn.voole.com:3528/play?fid=ff2d905b74ee3a7a5c8cbc352bc3fc20&keyid=0&stamp=1406184247&is3d=0&fm=7&tvid=BC83A71935BE&bit=1300&auth=6caba6f89099539475e09a81e9696066&ext=oid:433,eid:100105,code:ASTBOX_movie_index&s=1	1406184279	0	2874777552";
		inputValue = new BytesWritable(str.getBytes());

		_outKey = new CamusMapperTimeKeyAvro();
		_outKey.setTopic(OrderTopicsUtils.TOPIC_ORDER_BGN_V2);
		_outKey.setCategoryTime(df.parse("2014-07-24 00:00:00").getTime());
		outKey = new AvroKey<CamusMapperTimeKeyAvro>(_outKey);

		OrderPlayBgnReqV2 _outVal = getOrderPlayBgnReqV2();
		outVal = new AvroValue<SpecificRecordBase>(_outVal);
	}

	private OrderPlayBgnReqV2 getOrderPlayBgnReqV2() {
		OrderPlayBgnReqV2 v = new OrderPlayBgnReqV2();
		v.setOEMID(145l);
		v.setVendorID(0l);
		v.setCurVer(30l);
		v.setBuildTime(1305905550l);
		v.setHID("BC83A71935BE00000000000000000000");
		v.setUID("1000");
		v.setLocalIP(1694542016l);
		v.setSessID("8546749285776193522");
		v.setSessType(0);
		v.setSessStatus(0);
		v.setFID("FF2D905B74EE3A7A5C8CBC352BC3FC20");
		v.setMSize(1363914803l);
		v.setMmime(7);
		v.setIdxLen(992688l);
		v.setUrlLen(220);
		v.setURL("vosp://cdn.voole.com:3528/play?fid=ff2d905b74ee3a7a5c8cbc352bc3fc20&keyid=0&stamp=1406184247&is3d=0&fm=7&tvid=BC83A71935BE&bit=1300&auth=6caba6f89099539475e09a81e9696066&ext=oid:433,eid:100105,code:ASTBOX_movie_index&s=1");
		v.setPlayTick(1406184279l);
		v.setSrvNum(0);
		v.setNatip(2874777552l);

		return v;
	}

	@Test
	public void testMapper() throws IOException, ParseException {
		mapDriver.withInput(inputKey, inputValue).withOutput(outKey, outVal);
		mapDriver.runTest();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReducer() throws IOException, ParseException {
		reduceDriver.withInput(outKey, Lists.newArrayList(outVal)).withOutput(
				outKey, outVal);
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(inputKey, inputValue)
				.withOutput(outKey, outVal).runTest();
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

		Schema vaSchema = getMapValueSchema(conf);
		setMapOutputKeySchema(conf, CamusMapperTimeKeyAvro.getClassSchema());
		setOutputKeySchema(conf, CamusMapperTimeKeyAvro.getClassSchema());

		setMapOutputValueSchema(conf, vaSchema);
		setOutputValueSchema(conf, vaSchema);
		return true;
	}

	public static Schema getMapValueSchema(Configuration conf) {
		AvroSchemas avroSchemas = new AvroSchemas(conf.getInstances(
				CamusMetaConfigs.SCHEMA_REGISTERS, AvroSchemaRegister.class)
				.toArray(new AvroSchemaRegister[] {}));
		return Schema.createUnion(new ArrayList<Schema>(avroSchemas
				.getAllSchema()));
	}

}
