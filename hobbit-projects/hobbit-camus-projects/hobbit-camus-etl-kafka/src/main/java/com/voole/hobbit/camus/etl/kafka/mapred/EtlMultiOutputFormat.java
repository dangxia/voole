package com.voole.hobbit.camus.etl.kafka.mapred;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.coders.Partitioner;
import com.voole.hobbit.camus.etl.IEtlKey;
import com.voole.hobbit.camus.etl.RecordWriterProvider;
import com.voole.hobbit.camus.etl.kafka.CamusConfigs;
import com.voole.hobbit.camus.etl.kafka.common.DateUtils;
import com.voole.hobbit.camus.etl.kafka.common.EtlCounts;
import com.voole.hobbit.camus.etl.kafka.common.EtlKey;
import com.voole.hobbit.camus.etl.kafka.common.ExceptionWritable;
import com.voole.hobbit.camus.etl.kafka.util.SequenceFileUtils;

/**
 * MultipleAvroOutputFormat.
 * 
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
	public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
	public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

	public static final DateTimeFormatter FILE_DATE_FORMATTER = DateUtils
			.getDateTimeFormatter("YYYYMMdd");

	private static EtlMultiOutputCommitter committer = null;
	private static Map<String, Partitioner> partitionersByTopic = new HashMap<String, Partitioner>();

	private long granularityMs;
	private static Logger log = Logger.getLogger(EtlMultiOutputFormat.class);

	@Override
	public RecordWriter<EtlKey, Object> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (committer == null)
			committer = new EtlMultiOutputCommitter(getOutputPath(context),
					context);
		granularityMs = CamusConfigs.getMonitorTimeGranularityMins(context) * 60000L;
		return new MultiEtlRecordWriter(context);
	}

	private RecordWriter<IEtlKey, CamusWrapper<?>> getDataRecordWriter(
			TaskAttemptContext context, String fileName, CamusWrapper<?> value)
			throws IOException, InterruptedException {
		RecordWriterProvider recordWriterProvider = null;
		try {
			recordWriterProvider = CamusConfigs.getRecordWriterProviderClass(
					context).newInstance();
		} catch (InstantiationException e) {
			throw new IllegalStateException(e);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		return recordWriterProvider.getDataRecordWriter(context, fileName,
				value, committer);
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		if (committer == null)
			committer = new EtlMultiOutputCommitter(getOutputPath(context),
					context);
		granularityMs = CamusConfigs.getMonitorTimeGranularityMins(context) * 60 * 1000L;
		return committer;
	}

	public static boolean isRunMoveData(JobContext job) {
		return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
	}

	public static void setRunMoveData(JobContext job, boolean value) {
		job.getConfiguration().setBoolean(ETL_RUN_MOVE_DATA, value);
	}

	public static boolean isRunTrackingPost(JobContext job) {
		return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
	}

	public static void setRunTrackingPost(JobContext job, boolean value) {
		job.getConfiguration().setBoolean(ETL_RUN_TRACKING_POST, value);
	}

	public String getWorkingFileName(JobContext context, EtlKey key)
			throws IOException {
		Partitioner partitioner = getPartitioner(context, key.getTopic());
		return "data." + key.getTopic().replaceAll("\\.", "_") + "."
				+ key.getLeaderId() + "." + key.getPartition() + "."
				+ partitioner.encodePartition(context, key);
	}

	public static Partitioner getDefaultPartitioner(JobContext job) {
		if (partitionersByTopic.get(CamusConfigs.ETL_DEFAULT_PARTITIONER_CLASS) == null) {
			partitionersByTopic.put(CamusConfigs.ETL_DEFAULT_PARTITIONER_CLASS,
					CamusConfigs.getDefaultPartitioner(job));
		}
		return partitionersByTopic
				.get(CamusConfigs.ETL_DEFAULT_PARTITIONER_CLASS);
	}

	public static Partitioner getPartitioner(JobContext job, String topicName)
			throws IOException {
		if (partitionersByTopic.get(topicName) == null) {
			List<Partitioner> partitioners = CamusConfigs.getPartitioner(job,
					topicName);
			if (partitioners.isEmpty()) {
				return getDefaultPartitioner(job);
			} else {
				partitionersByTopic.put(topicName, partitioners.get(0));
			}
		}
		return partitionersByTopic.get(topicName);
	}

	public static void resetPartitioners() {
		partitionersByTopic = new HashMap<String, Partitioner>();
	}

	class MultiEtlRecordWriter extends RecordWriter<EtlKey, Object> {
		private TaskAttemptContext context;
		private Writer errorWriter = null;
		private String currentTopic = "";
		private long beginTimeStamp = 0;

		private HashMap<String, RecordWriter<IEtlKey, CamusWrapper<?>>> dataWriters = new HashMap<String, RecordWriter<IEtlKey, CamusWrapper<?>>>();

		public MultiEtlRecordWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.context = context;
			errorWriter = SequenceFileUtils.createWriter(
					FileSystem.get(context.getConfiguration()),
					context.getConfiguration(),
					new Path(committer.getWorkPath(), getUniqueFile(context,
							CamusConfigs.ERRORS_PREFIX, "")), EtlKey.class,
					ExceptionWritable.class);

			if (CamusConfigs.getKafkaMaxHistoricalDays(context) != -1) {
				int maxDays = CamusConfigs.getKafkaMaxHistoricalDays(context);
				beginTimeStamp = (new DateTime()).minusDays(maxDays)
						.getMillis();
			} else {
				beginTimeStamp = 0;
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for (String w : dataWriters.keySet()) {
				dataWriters.get(w).close(context);
			}
			errorWriter.close();
		}

		@Override
		public void write(EtlKey key, Object val) throws IOException,
				InterruptedException {
			if (val instanceof CamusWrapper<?>) {
				if (key.getTime() < beginTimeStamp) {
					// ((Mapper.Context)context).getCounter("total",
					// "skip-old").increment(1);
					committer.addOffset(key);
				} else {
					if (!key.getTopic().equals(currentTopic)) {
						for (RecordWriter<IEtlKey, CamusWrapper<?>> writer : dataWriters
								.values()) {
							writer.close(context);
						}
						dataWriters.clear();
						currentTopic = key.getTopic();
					}

					committer.addCounts(key);
					CamusWrapper<?> value = (CamusWrapper<?>) val;
					String workingFileName = getWorkingFileName(context, key);
					if (!dataWriters.containsKey(workingFileName)) {
						dataWriters.put(
								workingFileName,
								getDataRecordWriter(context, workingFileName,
										value));
					}
					dataWriters.get(workingFileName).write(key, value);
				}
			} else if (val instanceof ExceptionWritable) {
				committer.addOffset(key);
				System.err.println(key.toString());
				System.err.println(val.toString());
				errorWriter.append(key, (ExceptionWritable) val);
			}
		}
	}

	public class EtlMultiOutputCommitter extends FileOutputCommitter {
		Pattern workingFileMetadataPattern;

		HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
		HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();

		TaskAttemptContext context;
		private final RecordWriterProvider recordWriterProvider;

		public void addCounts(EtlKey key) throws IOException {
			String workingFileName = getWorkingFileName(context, key);
			if (!counts.containsKey(workingFileName))
				counts.put(workingFileName, new EtlCounts(key.getTopic(),
						granularityMs));
			counts.get(workingFileName).incrementMonitorCount(key);
			addOffset(key);
		}

		public void addOffset(EtlKey key) {
			String topicPart = key.getTopic() + "-" + key.getLeaderId() + "-"
					+ key.getPartition();
			offsets.put(topicPart, new EtlKey(key));
		}

		public EtlMultiOutputCommitter(Path outputPath,
				TaskAttemptContext context) throws IOException {
			super(outputPath, context);
			this.context = context;
			try {
				recordWriterProvider = CamusConfigs
						.getRecordWriterProviderClass(context).newInstance();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
			workingFileMetadataPattern = Pattern
					.compile("data\\.([^\\.]+)\\.(\\d+)\\.(\\d+)\\.([^\\.]+)-m-\\d+"
							+ recordWriterProvider.getFilenameExtension());
		}

		@Override
		public void commitTask(TaskAttemptContext context) throws IOException {

			ArrayList<Map<String, Object>> allCountObject = new ArrayList<Map<String, Object>>();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			if (isRunMoveData(context)) {
				Path workPath = super.getWorkPath();
				Path baseOutDir = CamusConfigs.getDestinationPath(context);
				for (FileStatus f : fs.listStatus(workPath)) {
					String file = f.getPath().getName();
					if (file.startsWith("data")) {
						String workingFileName = file.substring(0,
								file.lastIndexOf("-m"));
						EtlCounts count = counts.get(workingFileName);

						String partitionedFile = getPartitionedPath(context,
								file, count.getEventCount(), count.getLastKey()
										.getOffset());

						Path dest = new Path(baseOutDir, partitionedFile);

						if (!fs.exists(dest.getParent())) {
							fs.mkdirs(dest.getParent());
						}

						fs.rename(f.getPath(), dest);

						if (isRunTrackingPost(context)) {
							count.writeCountsToHDFS(
									allCountObject,
									fs,
									new Path(
											workPath,
											CamusConfigs.COUNTS_PREFIX
													+ "."
													+ dest.getName()
															.replace(
																	recordWriterProvider
																			.getFilenameExtension(),
																	"")));
						}
					}
				}

				Path tempPath = new Path(workPath, "counts."
						+ context.getConfiguration().get("mapred.task.id"));
				OutputStream outputStream = new BufferedOutputStream(
						fs.create(tempPath));
				ObjectMapper mapper = new ObjectMapper();
				log.info("Writing counts to : " + tempPath.toString());
				long time = System.currentTimeMillis();
				mapper.writeValue(outputStream, allCountObject);
				log.debug("Time taken : " + (System.currentTimeMillis() - time)
						/ 1000);
			}
			SequenceFile.Writer offsetWriter = SequenceFileUtils.createWriter(
					fs,
					context.getConfiguration(),
					new Path(super.getWorkPath(), getUniqueFile(context,
							CamusConfigs.OFFSET_PREFIX, "")), EtlKey.class,
					NullWritable.class);
			for (String s : offsets.keySet()) {
				offsetWriter.append(offsets.get(s), NullWritable.get());
			}
			offsetWriter.close();
			super.commitTask(context);
		}

		public String getPartitionedPath(JobContext context, String file,
				int count, long offset) throws IOException {
			Matcher m = workingFileMetadataPattern.matcher(file);
			if (!m.find()) {
				throw new IOException(
						"Could not extract metadata from working filename '"
								+ file + "'");
			}
			String topic = m.group(1);
			String leaderId = m.group(2);
			String partition = m.group(3);
			String encodedPartition = m.group(4);

			String partitionedPath = getPartitioner(context, topic)
					.generatePartitionedPath(context, topic,
							Integer.parseInt(leaderId),
							Integer.parseInt(partition), encodedPartition);

			return partitionedPath + "/" + topic + "." + leaderId + "."
					+ partition + "." + count + "." + offset + "."
					+ CamusConfigs.getJobExcutionStamp(context)
					+ recordWriterProvider.getFilenameExtension();
		}
	}
}
