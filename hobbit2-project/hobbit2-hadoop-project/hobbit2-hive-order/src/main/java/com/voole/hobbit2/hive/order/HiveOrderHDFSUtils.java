package com.voole.hobbit2.hive.order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.hobbit2.camus.OrderTopicsUtils;

public class HiveOrderHDFSUtils {
	private static Logger log = LoggerFactory
			.getLogger(HiveOrderHDFSUtils.class);

	public static Map<String, HiveTable> readFileNameToHiveTableMap(
			Path newExecutionOutput, JobContext context) throws IOException {
		Map<String, HiveTable> fileNameToHiveTableMap = new HashMap<String, HiveTable>();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FileStatus[] files = fs.listStatus(newExecutionOutput,
				new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().startsWith(
								HiveOrderMetaConfigs.NOEND_PREFIX);
					}
				});
		for (FileStatus fileStatus : files) {
			Reader r = new SequenceFile.Reader(context.getConfiguration(),
					SequenceFile.Reader.file(fileStatus.getPath()));
			Text filePath = new Text();
			HiveTable table = new HiveTable();
			while (r.next(filePath, table)) {
				fileNameToHiveTableMap.put(filePath.toString(), table);
				table = new HiveTable();
			}
			r.close();
		}
		return fileNameToHiveTableMap;

	}

	public static void writeFileNameToHiveTableMap(
			Map<String, HiveTable> fileNameToHiveTableMap, Path path,
			JobContext context) throws IOException {
		if (fileNameToHiveTableMap.size() > 0) {
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

	public static void checkAndLoadCamsuPath(Job job, Path destPath,
			String... topics) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Preconditions.checkArgument(fs.exists(destPath),
				"camus destPath does not exist");
		Preconditions.checkArgument(topics != null && topics.length > 0,
				"hive order topics is empty");
		for (String topic : topics) {
			if (!OrderTopicsUtils.containsTopic(topic)) {
				throw new UnsupportedOperationException("topic:" + topic
						+ " is not order topic!");
			}
			Path topicPath = new Path(destPath, topic);
			Preconditions.checkArgument(fs.exists(topicPath), "topic:" + topic
					+ " destPath does not exist");
			FileInputFormat.addInputPath(job, topicPath);
		}
	}

	public static void checkHiveOrderPath(Configuration conf,
			Path execBasePath, Path execHistoryPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (!fs.exists(execBasePath)) {
			log.info("The execution base path does not exist. Creating the directory");
			fs.mkdirs(execBasePath);
		}

		if (!fs.exists(execHistoryPath)) {
			log.info("The history base path does not exist. Creating the directory.");
			fs.mkdirs(execHistoryPath);
		}

	}

	public static void checkExecHistoryQuota(Configuration conf,
			Path execBasePath, Path execHistoryPath, float quota)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		ContentSummary content = fs.getContentSummary(execBasePath);
		long limit = (long) (content.getQuota() * quota);
		limit = limit == 0 ? 50000 : limit;

		long currentCount = content.getFileCount()
				+ content.getDirectoryCount();

		FileStatus[] executions = fs.listStatus(execHistoryPath);

		// removes oldest directory until we get under required % of count
		// quota. Won't delete the most recent directory.
		for (int i = 0; i < executions.length - 1 && limit < currentCount; i++) {
			FileStatus stat = executions[i];
			log.info("removing old execution: " + stat.getPath().getName());
			ContentSummary execContent = fs.getContentSummary(stat.getPath());
			currentCount -= execContent.getFileCount()
					- execContent.getDirectoryCount();
			fs.delete(stat.getPath(), true);
		}
	}

	public static void writeCurrCamusExecTime(JobContext job, long lastStartTime)
			throws IOException {
		Path currCamusMaxStamp = new Path(FileOutputFormat.getOutputPath(job),
				HiveOrderMetaConfigs.PREV_CAMUS_EXEC_TIME_FILE_NAME);

		Writer writer = SequenceFile.createWriter(job.getConfiguration(),
				Writer.file(currCamusMaxStamp),
				Writer.keyClass(NullWritable.class),
				Writer.valueClass(LongWritable.class));

		writer.append(NullWritable.get(), new LongWritable(lastStartTime));
		writer.close();
	}

	public static long readPrevCamusExecTime(JobContext job,
			Optional<Path> prevExecPath) throws IOException {
		if (!prevExecPath.isPresent()) {
			return 0l;
		}
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path prevCamusLastExecStartTimeFile = new Path(prevExecPath.get(),
				HiveOrderMetaConfigs.PREV_CAMUS_EXEC_TIME_FILE_NAME);
		Preconditions.checkArgument(fs.exists(prevCamusLastExecStartTimeFile),
				"PREV_CAMUS_EXEC_TIME_FILE_NAME does not exist");
		Reader r = new SequenceFile.Reader(job.getConfiguration(),
				SequenceFile.Reader.file(prevCamusLastExecStartTimeFile));
		LongWritable prevCamusLastExecStartTimeWritable = new LongWritable();
		long lastStartTime = 0;
		if (r.next(NullWritable.get(), prevCamusLastExecStartTimeWritable)) {
			lastStartTime = prevCamusLastExecStartTimeWritable.get();
		}
		r.close();
		return lastStartTime;
	}

	public static Optional<Path> getPrevExecPath(Configuration conf,
			Path execHistoryPath) throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] executions = fs.listStatus(execHistoryPath);
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			log.info("Previous execution: " + previous.toString());
			return Optional.of(previous);
		} else {
			log.info("No previous execution");
			return Optional.absent();
		}
	}
}
