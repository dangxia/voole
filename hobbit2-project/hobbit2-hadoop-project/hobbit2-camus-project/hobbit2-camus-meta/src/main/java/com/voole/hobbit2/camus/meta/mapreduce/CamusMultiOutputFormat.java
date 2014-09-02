/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.hadoop.file.HadoopCodecFactory;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.voole.hobbit2.camus.meta.CamusHDFSUtils;
import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.common.CamusKey;
import com.voole.hobbit2.kafka.common.partition.Partitioner;
import com.voole.hobbit2.kafka.common.partition.Partitioners;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月2日
 */
public class CamusMultiOutputFormat extends FileOutputFormat<CamusKey, Object> {
	public static class AvroFileStats {
		private final long beginOffset;
		private long lastOffset;

		public AvroFileStats(CamusKey key) {
			this.beginOffset = key.getOffset();
			this.lastOffset = this.beginOffset;
		}

		public long getBeginOffset() {
			return beginOffset;
		}

		public long getLastOffset() {
			return lastOffset;
		}

		public void setLastKey(CamusKey key) {
			this.lastOffset = key.getOffset();
		}

	}

	private static Logger log = LoggerFactory
			.getLogger(CamusMultiOutputFormat.class);

	private final Map<String, CamusKey> pathToMeta;
	private final Map<String, AvroFileStats> pathToFileStats;
	private CamusMultiOutputCommitter committer;

	private static Pattern p = Pattern.compile("(.*?)-[mr]-\\d+\\.\\w+$");
	private Partitioners partitioners;

	public CamusMultiOutputFormat() {
		pathToMeta = new HashMap<String, CamusKey>();
		pathToFileStats = new HashMap<String, AvroFileStats>();
	}

	@Override
	public RecordWriter<CamusKey, Object> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		if (committer == null) {
			Path output = getOutputPath(job);
			committer = new CamusMultiOutputCommitter(output, job);
		}
		initPartitioners(job);
		return new CamusMultiRecordWriter(job);
	}

	private void initPartitioners(TaskAttemptContext job) {
		if (this.partitioners == null) {
			this.partitioners = CamusMetaConfigs.getPartitioners(job);
		}
	}

	@Override
	public synchronized CamusMultiOutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new CamusMultiOutputCommitter(output, context);
		}
		initPartitioners(context);
		return committer;
	}

	class CamusMultiOutputCommitter extends FileOutputCommitter {

		public CamusMultiOutputCommitter(Path outputPath,
				TaskAttemptContext context) throws IOException {
			super(outputPath, context);
		}

		protected String findName(FileStatus fileStatus) {
			Matcher m = p.matcher(fileStatus.getPath().getName());
			m.find();
			return m.group(1);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected Path getDestPath(CamusKey key, AvroFileStats avroFileStats,
				TaskAttemptContext context) {
			String destName = getDestFileName(key, avroFileStats, context);
			Path destPath = CamusMetaConfigs.getDestPath(context);
			Partitioner p = partitioners.get(key.getTopic());
			return new Path(destPath, p.getPath(key) + destName);
		}

		protected String getDestFileName(CamusKey key,
				AvroFileStats avroFileStats, TaskAttemptContext context) {
			return Joiner.on('.').join(key.getTopic(), key.getPartition(),
					avroFileStats.getBeginOffset(),
					avroFileStats.getLastOffset(),
					CamusMetaConfigs.getExecStartTime(context), ".avro");
		}

		@Override
		public void commitTask(TaskAttemptContext context) throws IOException {
			FileSystem fs = getWorkPath().getFileSystem(
					context.getConfiguration());
			FileStatus[] fileStatus = fs.listStatus(getWorkPath(),
					new PathFilter() {
						@Override
						public boolean accept(Path path) {
							return path.getName().startsWith("data_");
						}
					});
			Map<TopicPartition, Long> prevOffsets = new HashMap<TopicPartition, Long>();
			for (FileStatus fileStatus2 : fileStatus) {
				String fileName = findName(fileStatus2);
				AvroFileStats avroFileStats = pathToFileStats.get(fileName);
				CamusKey key = pathToMeta.get(fileName);
				Path destPath = getDestPath(key, avroFileStats, context);
				fs.mkdirs(destPath.getParent());
				if (!fs.rename(fileStatus2.getPath(), destPath)) {
					log.info("sourcePath:" + fileStatus2.getPath()
							+ " rename to " + destPath + " failed");
				}

				TopicPartition topicPartition = new TopicPartition(
						key.getTopic(), key.getPartition());
				long oldOffset = 0l;
				if (prevOffsets.containsKey(topicPartition)) {
					oldOffset = prevOffsets.get(topicPartition);
				}
				if (avroFileStats.lastOffset > oldOffset) {
					prevOffsets.put(topicPartition, avroFileStats.lastOffset);
				}
			}
			CamusHDFSUtils.writePreviousOffsets(
					context.getConfiguration(),
					new Path(getWorkPath(), getUniqueFile(context,
							CamusMetaConfigs.OFFSET_PREFIX, ".offset")),
					prevOffsets);
			super.commitTask(context);
		}
	}

	class CamusMultiRecordWriter extends RecordWriter<CamusKey, Object> {
		private final Map<String, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> dataWriters;
		private final TaskAttemptContext attemptContext;
		private final AvroKey<SpecificRecordBase> record;
		private SequenceFile.Writer errorWriter;

		public CamusMultiRecordWriter(TaskAttemptContext attemptContext) {
			dataWriters = new HashMap<String, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>>();
			this.attemptContext = attemptContext;
			this.record = new AvroKey<SpecificRecordBase>();
		}

		protected String getWorkFileName(CamusKey key) {
			return Joiner.on('.').join("data_" + key.getTopic(),
					key.getPartition(), key.getBeginOffset(), key.getStamp());
		}

		@Override
		public void write(CamusKey key, Object value) throws IOException,
				InterruptedException {
			if (value instanceof SpecificRecordBase) {
				String fileName = getWorkFileName(key);
				if (!dataWriters.containsKey(fileName)) {
					dataWriters.put(
							fileName,
							createDataRecordWriter(attemptContext, fileName,
									key));
				}
				this.record.datum((SpecificRecordBase) value);
				pathToFileStats.get(fileName).setLastKey(key);
				dataWriters.get(fileName)
						.write(this.record, NullWritable.get());
			} else {
				getErrorWriter().append(key, (Text) value);
			}

		}

		protected SequenceFile.Writer getErrorWriter() throws IOException {
			if (errorWriter == null) {
				Path errorPath = new Path(committer.getWorkPath(),

				FileOutputFormat.getUniqueFile(attemptContext,
						CamusMetaConfigs.ERRORS_PREFIX, ".error"));
				errorWriter = SequenceFile.createWriter(
						attemptContext.getConfiguration(),
						Writer.file(errorPath),
						Writer.keyClass(CamusKey.class),
						Writer.valueClass(Text.class),
						Writer.compression(CompressionType.NONE));
			}
			return errorWriter;
		}

		private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> createDataRecordWriter(
				TaskAttemptContext context, String fileName, CamusKey key)
				throws IOException, InterruptedException {
			pathToMeta.put(fileName, new CamusKey(key));
			pathToFileStats.put(fileName, new AvroFileStats(key));

			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = getOutputCommitter(context).getWorkPath();
			fileName = getUniqueFile(context, fileName, ".avro");
			path = new Path(path, fileName);

			log.info("create file: " + path);

			return new AvroKeyRecordWriter<SpecificRecordBase>(CamusMetaConfigs
					.getAvroSchemas(context).getSchema(key.getTopic()),
					AvroSerialization.createDataModel(context
							.getConfiguration()), getCompressionCodec(context),
					fs.create(path), getSyncInterval(context));
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for (Entry<String, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> entry : dataWriters
					.entrySet()) {
				entry.getValue().close(context);
			}
			if (errorWriter != null) {
				errorWriter.close();
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
