/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;
import com.voole.hobbit2.kafka.common.partition.Partitioner;
import com.voole.hobbit2.kafka.common.partition.Partitioners;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class CamusMultiOutputFormat
		extends
		FileOutputFormat<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
	private static Logger log = LoggerFactory
			.getLogger(CamusMultiOutputFormat.class);

	private final Map<CamusMapperTimeKeyAvro, Long> partitionToTotal;
	private final Map<Path, CamusMapperTimeKeyAvro> pathToMeta;
	private CamusMultiOutputCommitter committer;
	private Partitioners partitioners;

	public CamusMultiOutputFormat() {
		partitionToTotal = new HashMap<CamusMapperTimeKeyAvro, Long>();
		pathToMeta = new HashMap<Path, CamusMapperTimeKeyAvro>();
	}

	@Override
	public RecordWriter<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		if (committer == null) {
			Path output = getOutputPath(job);
			committer = new CamusMultiOutputCommitter(output, job);
		}
		return new CamusMultiRecordWriter(job);
	}

	@Override
	public synchronized CamusMultiOutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new CamusMultiOutputCommitter(output, context);
		}
		return committer;
	}

	public Partitioners getPartitioners(TaskAttemptContext context) {
		if (partitioners == null) {
			partitioners = CamusMetaConfigs.getPartitioners(context);
		}
		return partitioners;
	}

	class CamusMultiOutputCommitter extends FileOutputCommitter {

		public CamusMultiOutputCommitter(Path outputPath,
				TaskAttemptContext context) throws IOException {
			super(outputPath, context);
		}

		@Override
		public void commitTask(TaskAttemptContext context) throws IOException {
			log.info("CamusMultiOutputCommitter workPath:" + getWorkPath());
			for (Entry<Path, CamusMapperTimeKeyAvro> entry : pathToMeta
					.entrySet()) {
				CamusMapperTimeKeyAvro key = entry.getValue();
				Path sourcePath = entry.getKey();
				long count = partitionToTotal.get(entry.getValue());
				String destName = key.getTopic() + "_" + count + "_"
						+ CamusMetaConfigs.getExecStartTime(context);
				Path destPath = CamusMetaConfigs.getDestPath(context);

				@SuppressWarnings("unchecked")
				Path targetPath = new Path(
						destPath,
						((Partitioner<CamusMapperTimeKeyAvro, ?, ?>) getPartitioners(
								context).get(key.getTopic().toString()))
								.getPath(key) + destName + ".avro");
				FileSystem fs = FileSystem.get(context.getConfiguration());
				fs.mkdirs(targetPath.getParent());
				if (!fs.rename(sourcePath, targetPath)) {
					log.info("sourcePath:" + sourcePath + " rename to "
							+ targetPath + " failed");
				}
			}
			super.commitTask(context);
		}
	}

	class CamusMultiRecordWriter
			extends
			RecordWriter<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
		private final Map<CamusMapperTimeKeyAvro, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> dataWriters;
		private final TaskAttemptContext attemptContext;
		private final AvroKey<SpecificRecordBase> record;

		public CamusMultiRecordWriter(TaskAttemptContext attemptContext) {
			dataWriters = new HashMap<CamusMapperTimeKeyAvro, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>>();
			this.attemptContext = attemptContext;
			this.record = new AvroKey<SpecificRecordBase>();
		}

		@Override
		public void write(AvroKey<CamusMapperTimeKeyAvro> keyOld,
				AvroValue<SpecificRecordBase> value) throws IOException,
				InterruptedException {
			CamusMapperTimeKeyAvro avroKey = new CamusMapperTimeKeyAvro();
			avroKey.setTopic(keyOld.datum().getTopic());
			avroKey.setCategoryTime(keyOld.datum().getCategoryTime());

			if (!dataWriters.containsKey(avroKey)) {
				dataWriters.put(avroKey,
						getDataRecordWriter(attemptContext, avroKey, value));
				partitionToTotal.put(avroKey, 0l);
			}
			partitionToTotal.put(avroKey, partitionToTotal.get(avroKey) + 1);
			this.record.datum(value.datum());
			dataWriters.get(avroKey).write(this.record, NullWritable.get());
		}

		private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> getDataRecordWriter(
				TaskAttemptContext context, CamusMapperTimeKeyAvro key,
				AvroValue<SpecificRecordBase> value) throws IOException,
				InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = getOutputCommitter(context).getWorkPath();
			String name = "data_" + key.getTopic() + "_"
					+ key.getCategoryTime();
			name = getUniqueFile(context, name, ".avro");
			path = new Path(path, name);
			pathToMeta.put(path, key);

			return new AvroKeyRecordWriter<SpecificRecordBase>(CamusMetaConfigs
					.getAvroSchemas(context).getSchema(
							key.getTopic().toString()),
					AvroSerialization.createDataModel(context
							.getConfiguration()), getCompressionCodec(context),
					new BufferedOutputStream(fs.create(path)),
					getSyncInterval(context));
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for (Entry<CamusMapperTimeKeyAvro, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> entry : dataWriters
					.entrySet()) {
				entry.getValue().close(context);
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
