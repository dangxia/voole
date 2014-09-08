package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.hadoop.file.HadoopCodecFactory;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;

public class HiveOrderMultiOutputFormat extends
		FileOutputFormat<Object, Object> {
	private static Logger log = LoggerFactory
			.getLogger(HiveOrderMultiOutputFormat.class);

	@Override
	public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return new HiveOrderMultiRecordWriter(job);
	}

	@Override
	public synchronized FileOutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		return (FileOutputCommitter) super.getOutputCommitter(context);
	}

	class HiveOrderMultiRecordWriter extends RecordWriter<Object, Object> {
		private final Map<String, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>> dataWriters;
		private final Map<String, HiveTable> fileNameToHiveTableMap;
		private final TaskAttemptContext attemptContext;
		private final AvroKey<SpecificRecordBase> record;

		public HiveOrderMultiRecordWriter(TaskAttemptContext job) {
			this.attemptContext = job;
			dataWriters = new HashMap<String, RecordWriter<AvroKey<SpecificRecordBase>, NullWritable>>();
			this.record = new AvroKey<SpecificRecordBase>();
			fileNameToHiveTableMap = new HashMap<String, HiveTable>();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void write(Object key, Object value) throws IOException,
				InterruptedException {
			if (key instanceof HiveTable) {// 正常值
				HiveTable table = (HiveTable) key;
				List<SpecificRecordBase> data = (List<SpecificRecordBase>) value;
				String fileName = table.getFileName();
				RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> writer = null;
				if (!dataWriters.containsKey(fileName)) {
					writer = createDataRecordWriter(attemptContext, fileName,
							table);
					dataWriters.put(fileName, writer);
				} else {
					writer = dataWriters.get(fileName);
				}

				for (SpecificRecordBase specificRecordBase : data) {
					this.record.datum(specificRecordBase);
					writer.write(this.record, NullWritable.get());
				}
			} else {// noend

			}
		}

		private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> createDataRecordWriter(
				TaskAttemptContext context, String fileName, HiveTable table)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = getOutputCommitter(context).getWorkPath();
			fileName = getUniqueFile(context, fileName, ".avro");
			fileNameToHiveTableMap.put(fileName, table);
			path = new Path(path, fileName);

			log.info("create file: " + path);

			return new AvroKeyRecordWriter<SpecificRecordBase>(
					table.getSchema(),
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
			if (fileNameToHiveTableMap.size() > 0) {
				Path workPath = getOutputCommitter(context).getWorkPath();
				String fileName = getUniqueFile(context,
						HiveOrderMetaConfigs.FILE_INFO_PREFIX, ".fileInfo");
				Path path = new Path(workPath, fileName);
				Writer writer = SequenceFile.createWriter(
						context.getConfiguration(), Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(HiveTable.class));
				Text filePath = new Text();
				for (Entry<String, HiveTable> entry : fileNameToHiveTableMap
						.entrySet()) {
					filePath.set(entry.getKey());
					writer.append(filePath, entry.getValue());
				}
				writer.close();
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
