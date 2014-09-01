/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.voole.hobbit2.camus.meta.CamusHDFSUtils;
import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusKafkaKey;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.partition.Partitioner;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

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

	private TopicPartition topicPartition;
	private String topic;
	private long maxOffset = -1;

	private SequenceFile.Writer errorWriter;
	private Text errorText;

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		topicPartition = findTopicPartition(context);
		topic = topicPartition.getTopic();

		transformer = (KafkaTransformer<SpecificRecordBase>) CamusMetaConfigs
				.getTopicTransformMetas(context).getTransformer(topic);
		partitioner = (Partitioner<CamusMapperTimeKeyAvro, CamusKafkaKey, SpecificRecordBase>) CamusMetaConfigs
				.getPartitioners(context).get(topic);
		avrokey = new CamusMapperTimeKeyAvro();
		avrokey.setTopic(topic);
		_key = new AvroKey<CamusMapperTimeKeyAvro>(avrokey);
		_value = new AvroValue<SpecificRecordBase>();

		errorText = new Text();
	}

	protected SequenceFile.Writer getErrorWriter(Context context)
			throws IOException {
		if (errorWriter == null) {
			Path errorPath = new Path(getWorkingDirectory(context),
					FileOutputFormat.getUniqueFile(context,
							CamusMetaConfigs.ERRORS_PREFIX + "_" + topic,
							".error"));
			errorWriter = SequenceFile.createWriter(context.getConfiguration(),
					Writer.file(errorPath),
					Writer.keyClass(CamusKafkaKey.class),
					Writer.valueClass(Text.class),
					Writer.compression(CompressionType.NONE));
		}
		return errorWriter;
	}

	protected TopicPartition findTopicPartition(Context context) {
		return ((CamusInputSplit) context.getInputSplit())
				.getBrokerAndTopicPartition().getPartition();
	}

	protected Path getWorkingDirectory(Context context) throws IOException {
		return ((FileOutputCommitter) context.getOutputCommitter())
				.getWorkPath();
	}

	@Override
	protected void map(CamusKafkaKey key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.getCounter("fetch_topic_num", key.getPartition().getTopic())
				.increment(1l);
		if (maxOffset < key.getOffset()) {
			maxOffset = key.getOffset();
		}
		byte[] bv = null;
		try {
			bv = getBytes(value);
			SpecificRecordBase v = transformer.transform(bv);
			partitioner.partition(avrokey, key, v);
			_value.datum(v);
			context.write(_key, _value);
		} catch (Exception e) {
			context.getCounter("transform_failed",
					key.getPartition().getTopic()).increment(1l);
			errorText.set(value.getBytes());
			getErrorWriter(context).append(key, errorText);
		}
	}

	private static byte[] getBytes(BytesWritable val) {
		byte[] buffer = val.getBytes();
		long len = val.getLength();
		byte[] bytes = buffer;
		if (len < buffer.length) {
			bytes = new byte[(int) len];
			System.arraycopy(buffer, 0, bytes, 0, (int) len);
		}
		return bytes;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (errorWriter != null) {
			errorWriter.close();
		}
		if (maxOffset != -1) {
			Map<TopicPartition, Long> previousOffset = new HashMap<TopicPartition, Long>();
			previousOffset.put(topicPartition, maxOffset);
			CamusHDFSUtils.writePreviousOffsets(
					context.getConfiguration(),
					new Path(getWorkingDirectory(context), FileOutputFormat
							.getUniqueFile(
									context,
									CamusMetaConfigs.OFFSET_PREFIX + "_"
											+ topic + "_"
											+ topicPartition.getPartition(),
									".offset")), previousOffset);
		}
	}
}
