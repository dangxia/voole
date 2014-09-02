/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
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
	private final Map<AvroKey<CamusMapperTimeKeyAvro>, Long> partitionToTotal;
	private final Map<Path, AvroKey<CamusMapperTimeKeyAvro>> pathToMeta;
	private CamusMultiOutputCommitter committer;
	private static SimpleDateFormat df = new SimpleDateFormat("/yyyy/MM/dd/");

	public CamusMultiOutputFormat() {
		partitionToTotal = new HashMap<AvroKey<CamusMapperTimeKeyAvro>, Long>();
		pathToMeta = new HashMap<Path, AvroKey<CamusMapperTimeKeyAvro>>();
	}

	@Override
	public RecordWriter<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		return new CamusMultiRecordWriter(job);
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new CamusMultiOutputCommitter(output, context);
		}
		return committer;
	}

	class CamusMultiOutputCommitter extends FileOutputCommitter {

		public CamusMultiOutputCommitter(Path outputPath,
				TaskAttemptContext context) throws IOException {
			super(outputPath, context);
		}

		@Override
		public void commitTask(TaskAttemptContext context) throws IOException {
			for (Entry<Path, AvroKey<CamusMapperTimeKeyAvro>> entry : pathToMeta
					.entrySet()) {
				CamusMapperTimeKeyAvro key = entry.getValue().datum();
				Path sourcePath = entry.getKey();
				long count = partitionToTotal.get(entry.getValue());
				String destName = key.getTopic() + "_" + count + "_"
						+ CamusMetaConfigs.getExecStartTime(context);
				Path destPath = CamusMetaConfigs.getDestPath(context);
				Path targetPath = new Path(destPath, key.getTopic()
						+ df.format(new Date(key.getCategoryTime())) + destName
						+ ".avro");
				FileSystem fs = FileSystem.get(context.getConfiguration());
				fs.mkdirs(targetPath.getParent());
				fs.rename(sourcePath, targetPath);
			}
			super.commitTask(context);
		}
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
				partitionToTotal.put(key, 0l);
			}
			partitionToTotal.put(key, partitionToTotal.get(key) + 1);
			this.record.datum(value.datum());
			dataWriters.get(key).write(this.record, NullWritable.get());
		}

		private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> getDataRecordWriter(
				TaskAttemptContext context,
				AvroKey<CamusMapperTimeKeyAvro> key,
				AvroValue<SpecificRecordBase> value) throws IOException,
				InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = ((FileOutputCommitter) getOutputCommitter(context))
					.getWorkPath();
			CamusMapperTimeKeyAvro _key = key.datum();
			String name = "data_" + _key.getTopic() + "_"
					+ _key.getCategoryTime();
			name = getUniqueFile(context, name, ".avro");
			path = new Path(path, name);
			pathToMeta.put(path, key);

			return new AvroKeyRecordWriter<SpecificRecordBase>(CamusMetaConfigs
					.getAvroSchemas(context).getSchema(
							_key.getTopic().toString()),
					AvroSerialization.createDataModel(context
							.getConfiguration()), getCompressionCodec(context),
					new BufferedOutputStream(fs.create(path)),
					getSyncInterval(context));
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
