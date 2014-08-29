/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.hadoop.file.HadoopCodecFactory;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class CamusMultiOutputFormat
		extends
		FileOutputFormat<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {

	public CamusMultiOutputFormat() {

	}

	@Override
	public RecordWriter<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		return new CamusMultiRecordWriter(job);
	}

	class CamusMultiRecordWriter
			extends
			RecordWriter<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
		private final Map<AvroKey<CamusMapperTimeKeyAvro>, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> dataWriters;
		private final TaskAttemptContext attemptContext;
		private final AvroKey<SpecificRecordBase> record;

		public CamusMultiRecordWriter(TaskAttemptContext attemptContext) {
			dataWriters = new HashMap<AvroKey<CamusMapperTimeKeyAvro>, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>>();
			this.attemptContext = attemptContext;
			this.record = new AvroKey<SpecificRecordBase>();
		}

		@Override
		public void write(AvroKey<CamusMapperTimeKeyAvro> key,
				AvroValue<SpecificRecordBase> value) throws IOException,
				InterruptedException {
			if (!dataWriters.containsKey(key)) {
				dataWriters.put(key,
						getDataRecordWriter(attemptContext, key, value));
			}
			this.record.datum(value.datum());
			dataWriters.get(key).write(this.record, NullWritable.get());
		}

		private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> getDataRecordWriter(
				TaskAttemptContext context,
				AvroKey<CamusMapperTimeKeyAvro> key,
				AvroValue<SpecificRecordBase> value) throws IOException,
				InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = context.getWorkingDirectory();
			CamusMapperTimeKeyAvro _key = key.datum();
			String name = _key.getTopic() + "_" + _key.getCategoryTime();
			name = getUniqueFile(context, name, ".avro");
			path = new Path(path, name);

			return new AvroKeyRecordWriter<SpecificRecordBase>(CamusMetaConfigs
					.getAvroSchemas(context).getSchema(
							_key.getTopic().toString()),
					AvroSerialization.createDataModel(context
							.getConfiguration()), getCompressionCodec(context),
					fs.create(path), getSyncInterval(context));
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for (Iterator<Entry<AvroKey<CamusMapperTimeKeyAvro>, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>>> iterator = dataWriters
					.entrySet().iterator(); iterator.hasNext();) {
				Entry<AvroKey<CamusMapperTimeKeyAvro>, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> entry = iterator
						.next();
				entry.getValue().close(context);
				iterator.remove();
			}

		}

	}

	protected static CodecFactory getCompressionCodec(TaskAttemptContext context) {
		if (FileOutputFormat.getCompressOutput(context)) {
			// Default to deflate compression.
			int deflateLevel = context.getConfiguration().getInt(
					org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
					CodecFactory.DEFAULT_DEFLATE_LEVEL);
			int xzLevel = context.getConfiguration().getInt(
					org.apache.avro.mapred.AvroOutputFormat.XZ_LEVEL_KEY,
					CodecFactory.DEFAULT_XZ_LEVEL);

			String outputCodec = context.getConfiguration().get(
					AvroJob.CONF_OUTPUT_CODEC);

			if (outputCodec == null) {
				String compressionCodec = context.getConfiguration().get(
						"mapred.output.compression.codec");
				String avroCodecName = HadoopCodecFactory
						.getAvroCodecName(compressionCodec);
				if (avroCodecName != null) {
					context.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC,
							avroCodecName);
					return HadoopCodecFactory
							.fromHadoopString(compressionCodec);
				} else {
					return CodecFactory.deflateCodec(deflateLevel);
				}
			} else if (DataFileConstants.DEFLATE_CODEC.equals(outputCodec)) {
				return CodecFactory.deflateCodec(deflateLevel);
			} else if (DataFileConstants.XZ_CODEC.equals(outputCodec)) {
				return CodecFactory.xzCodec(xzLevel);
			} else {
				return CodecFactory.fromString(outputCodec);
			}

		}

		// No compression.
		return CodecFactory.nullCodec();
	}

	protected static int getSyncInterval(TaskAttemptContext context) {
		return context.getConfiguration().getInt(
				org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY,
				DataFileConstants.DEFAULT_SYNC_INTERVAL);
	}

}
