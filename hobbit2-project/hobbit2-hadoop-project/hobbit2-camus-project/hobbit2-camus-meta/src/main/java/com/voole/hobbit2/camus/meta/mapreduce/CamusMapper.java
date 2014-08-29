/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.partition.Partitioner;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusMapper
		extends
		Mapper<CamusKafkaKey, BytesWritable, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
	private KafkaTransformer<SpecificRecordBase> transformer;
	private Partitioner<CamusMapperTimeKeyAvro, CamusKafkaKey, SpecificRecordBase> partitioner;

	private AvroKey<CamusMapperTimeKeyAvro> _key;
	private CamusMapperTimeKeyAvro avrokey;
	private AvroValue<SpecificRecordBase> _value;
	private String topic;
	private SequenceFile.Writer errorWriter;

	private Text errorText;

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		errorText = new Text();
		topic = findTopic(context);
		transformer = (KafkaTransformer<SpecificRecordBase>) CamusMetaConfigs
				.getTopicTransformMetas(context).getTransformer(topic);
		partitioner = (Partitioner<CamusMapperTimeKeyAvro, CamusKafkaKey, SpecificRecordBase>) CamusMetaConfigs
				.getPartitioners(context).get(topic);
		avrokey = new CamusMapperTimeKeyAvro();
		avrokey.setTopic(topic);
		_key = new AvroKey<CamusMapperTimeKeyAvro>(avrokey);
		_value = new AvroValue<SpecificRecordBase>();
		Path errorPath = new Path(getWorkingDirectory(context),
				FileOutputFormat.getUniqueFile(context,
						CamusMetaConfigs.ERRORS_PREFIX, ".error"));
		System.out.println(errorPath);
		errorWriter = SequenceFile.createWriter(context.getConfiguration(),
				Writer.file(errorPath), Writer.keyClass(CamusKafkaKey.class),
				Writer.valueClass(Text.class));

	}

	protected String findTopic(Context context) {
		return ((CamusInputSplit) context.getInputSplit())
				.getBrokerAndTopicPartition().getPartition().getTopic();
	}

	protected Path getWorkingDirectory(Context context) throws IOException {
		return context.getWorkingDirectory();
	}

	@Override
	protected void map(CamusKafkaKey key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		try {
			SpecificRecordBase v = transformer.transform(value.getBytes());
			partitioner.partition(avrokey, key, v);
			_value.datum(v);
			context.write(_key, _value);
		} catch (Exception e) {
			errorText.set(value.getBytes());
			errorWriter.append(key, errorText);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		errorWriter.close();
	}

}
