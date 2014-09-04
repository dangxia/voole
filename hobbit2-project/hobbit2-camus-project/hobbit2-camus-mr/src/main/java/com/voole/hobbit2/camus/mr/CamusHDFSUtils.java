/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr;

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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月26日
 */
public class CamusHDFSUtils {
	private static org.apache.log4j.Logger log = Logger
			.getLogger(CamusHDFSUtils.class);

	public static void checkCamsuPath(Configuration conf, Path destPath,
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

		if (!fs.exists(destPath)) {
			log.info("The dest base path does not exist. Creating the directory.");
			fs.mkdirs(destPath);
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

	public static void writePrevPartionsStates(Configuration conf, Path path,
			Map<TopicPartition, Long> partitionStates) throws IOException {
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(path), Writer.keyClass(TopicPartition.class),
				Writer.valueClass(LongWritable.class));
		LongWritable offset = new LongWritable();
		for (Entry<TopicPartition, Long> entry : partitionStates.entrySet()) {
			offset.set(entry.getValue());
			writer.append(entry.getKey(), offset);
		}
		writer.close();
	}

	public static Map<TopicPartition, Long> readPrevPartionsStates(
			Configuration conf, Optional<Path> previousPath) throws IOException {
		return readPrevPartionsStates(conf, previousPath,
				CamusMetaConfigs.REQUESTS_FILE);

	}

	public static Map<TopicPartition, Long> readPrevPartionsStates(
			Configuration conf, Optional<Path> previousPath, String name)
			throws IOException {
		Map<TopicPartition, Long> result = new HashMap<TopicPartition, Long>();
		if (!previousPath.isPresent()) {
			return result;
		}
		Path path = new Path(previousPath.get(), name);
		if (!FileSystem.get(conf).exists(path)) {
			log.warn("file:" + name + "don't exists");
			return result;
		}
		log.info("readPrevPartionsStates file:" + path.toUri().getPath());
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		TopicPartition key = new TopicPartition();
		LongWritable offset = new LongWritable();
		while (reader.next(key, offset)) {
			long oldOffset = 0;
			if (result.containsKey(key)) {
				oldOffset = result.get(key);
			}
			if (oldOffset < offset.get()) {
				result.put(key, offset.get());
			}
			key = new TopicPartition();
		}
		reader.close();
		return result;

	}

	public static Optional<Path> getPreviousExecPath(Configuration conf,
			Path execHistoryPath) throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] executions = fs.listStatus(execHistoryPath);
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			log.info("Previous execution: " + previous.toString());
			return Optional.of(previous);
		} else {
			log.info("No previous execution, all topics pulled from earliest available offset");
			return Optional.absent();
		}
	}

	public static Map<TopicPartition, Long> readPreviousOffsets(
			Configuration conf, Optional<Path> previousPath) throws IOException {
		return readPreviousOffsets(conf, previousPath, new OffsetFileFilter());
	}

	public static Map<TopicPartition, Long> readMixedPreviousOffsets(
			Configuration conf, Path execHistoryPath)
			throws FileNotFoundException, IOException {
		Optional<Path> previousPath = getPreviousExecPath(conf, execHistoryPath);
		return readMixedPreviousOffsets(conf, previousPath);
	}

	public static Map<TopicPartition, Long> readMixedPreviousOffsets(
			Configuration conf, Optional<Path> previousPath) throws IOException {
		Map<TopicPartition, Long> map1 = readPrevPartionsStates(conf,
				previousPath);
		Map<TopicPartition, Long> map2 = readPreviousOffsets(conf, previousPath);
		for (Entry<TopicPartition, Long> entry : map2.entrySet()) {
			TopicPartition key = entry.getKey();
			long offset2 = entry.getValue();
			if (map1.containsKey(key)) {
				long offset1 = map1.get(key);
				if (offset1 < offset2) {
					map1.put(key, offset2);
				}
			} else {
				map1.put(key, offset2);
			}
		}

		return map1;
	}

	public static Map<TopicPartition, Long> readPreviousOffsets(
			Configuration conf, Optional<Path> previousPath, PathFilter filter)
			throws IOException {
		Map<TopicPartition, Long> result = new HashMap<TopicPartition, Long>();
		if (!previousPath.isPresent()) {
			return result;
		}
		FileSystem fs = FileSystem.get(conf);
		for (FileStatus f : fs.listStatus(previousPath.get(), filter)) {
			log.info("previous offset file:" + f.getPath().toString());
			SequenceFile.Reader reader = new SequenceFile.Reader(conf,
					Reader.file(f.getPath()));
			TopicPartition key = new TopicPartition();
			LongWritable value = new LongWritable();
			while (reader.next(key, value)) {
				long oldOffset = 0;
				if (result.containsKey(key)) {
					oldOffset = result.get(key);
				}
				if (oldOffset < value.get()) {
					result.put(key, value.get());
				}
				key = new TopicPartition();
			}
			reader.close();
		}

		return result;
	}

	public static void writePreviousOffsets(Configuration conf, Path path,
			Map<TopicPartition, Long> prevOffsets) throws IOException {
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(path), Writer.keyClass(TopicPartition.class),
				Writer.valueClass(LongWritable.class));
		LongWritable offset = new LongWritable();
		for (Entry<TopicPartition, Long> entry : prevOffsets.entrySet()) {
			offset.set(entry.getValue());
			writer.append(entry.getKey(), offset);
		}
		writer.close();
	}

	private static class OffsetFileFilter implements PathFilter {
		@Override
		public boolean accept(Path arg0) {
			return arg0.getName().startsWith(CamusMetaConfigs.OFFSET_PREFIX);
		}
	}
}
