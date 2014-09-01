/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import static com.voole.hobbit2.camus.meta.AvroJobTest.setMapOutputKeySchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setMapOutputValueSchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setOutputKeySchema;
import static com.voole.hobbit2.camus.meta.AvroJobTest.setOutputValueSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificDatumWriter;
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
import com.voole.hobbit2.camus.meta.avro.TestRecord;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.camus.meta.mapreduce.CamusMapper;
import com.voole.hobbit2.camus.meta.mapreduce.CamusReducer;
import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.kafka.avro.AvroSchemas;
import com.voole.hobbit2.kafka.avro.AvroSchemas.AvroSchemaRegister;
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
	private static final SimpleDateFormat df2 = new SimpleDateFormat(
			"yyyy-MM-dd 00:00:00");

	private CamusKafkaKey inputKey;
	private BytesWritable inputValue;
	private CamusMapperTimeKeyAvro _outKey;
	private AvroKey<CamusMapperTimeKeyAvro> outKey;
	private AvroValue<SpecificRecordBase> outVal;
	private String topic;

	public static byte[] getTestRecordBytes() throws IOException {
		ByteArrayOutputStream _out = new ByteArrayOutputStream();
		DatumWriter<TestRecord> userDatumWriter = new SpecificDatumWriter<TestRecord>(
				TestRecord.class);
		DataFileWriter<TestRecord> dataFileWriter = new DataFileWriter<TestRecord>(
				userDatumWriter);
		dataFileWriter.create(TestRecord.getClassSchema(), _out);
		dataFileWriter.append(getTestRecord());
		dataFileWriter.close();
		return _out.toByteArray();
	}

	private static TestRecord getTestRecord() {
		TestRecord r = new TestRecord();
		r.setName("test_name");
		r.setStamp(System.currentTimeMillis());
		return r;
	}

	@Before
	public void setUp() throws ParseException, IOException {
		topic = "test_record";
		CamusMapper mapper = new CamusMapper() {
			@Override
			protected TopicPartition findTopicPartition(Context context) {
				return new TopicPartition(topic, 2);
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
		inputKey.setPartition(new TopicPartition(topic, 2));
		inputValue = new BytesWritable(getTestRecordBytes());

		_outKey = new CamusMapperTimeKeyAvro();
		_outKey.setTopic(topic);
		_outKey.setCategoryTime(getDayTime());
		outKey = new AvroKey<CamusMapperTimeKeyAvro>(_outKey);

		outVal = new AvroValue<SpecificRecordBase>(getTestRecord());
	}

	private long getDayTime() throws ParseException {
		return df.parse(df2.format(new Date())).getTime();
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
