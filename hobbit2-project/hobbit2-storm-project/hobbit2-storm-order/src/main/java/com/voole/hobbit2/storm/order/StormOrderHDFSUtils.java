/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.voole.hobbit2.storm.order.partition.StormOrderSpoutPartitionCreator;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class StormOrderHDFSUtils {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderSpoutPartitionCreator.class);
	private static final Configuration conf = new Configuration();
	static {
		conf.addResource("core-site2.xml");
	}

	public static void main(String[] args) throws IOException {
		FileSystem.get(conf);
	}

	public static FileReader<SpecificRecordBase> getNoendReader(Path path)
			throws IOException {
		SeekableInput input = new FsInput(path, conf);
		DatumReader<SpecificRecordBase> reader = new SpecificDatumReader<SpecificRecordBase>();
		FileReader<SpecificRecordBase> fileReader = DataFileReader.openReader(
				input, reader);
		return fileReader;
	}

	private static Map<TopicPartition, Long> readPrevPartionsStates(
			Optional<Path> previousPath) throws IOException {
		return readPrevPartionsStates(previousPath,
				StormOrderMetaConfigs.CAMUS_REQUESTS_FILE);

	}

	private static Map<TopicPartition, Long> readPrevPartionsStates(
			Optional<Path> previousPath, String name) throws IOException {
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

	private static Optional<Path> getPreviousExecPath(Path execHistoryPath)
			throws FileNotFoundException, IOException {
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

	private static Map<TopicPartition, Long> readPreviousOffsets(
			Optional<Path> previousPath) throws IOException {
		return readPreviousOffsets(previousPath, new OffsetFileFilter());
	}

	public static Map<TopicPartition, Long> readMixedPreviousOffsets()
			throws FileNotFoundException, IOException {
		Optional<Path> previousPath = getPreviousExecPath(StormOrderMetaConfigs
				.getCamusExecHistoryPath());
		return readMixedPreviousOffsets(previousPath);
	}

	private static Map<TopicPartition, Long> readMixedPreviousOffsets(
			Optional<Path> previousPath) throws IOException {
		Map<TopicPartition, Long> map1 = readPrevPartionsStates(previousPath);
		Map<TopicPartition, Long> map2 = readPreviousOffsets(previousPath);
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

	private static Map<TopicPartition, Long> readPreviousOffsets(
			Optional<Path> previousPath, PathFilter filter) throws IOException {
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

	@SuppressWarnings("unchecked")
	public static List<Path> getNoendFilePaths() throws FileNotFoundException,
			IOException {
		final Set<String> topics = new HashSet<String>(
				StormOrderMetaConfigs.getWhiteTopics());
		Optional<Path> prevExecPath = getPreviousExecPath(StormOrderMetaConfigs
				.getHiveOrderExecHistoryPath());
		if (!prevExecPath.isPresent()) {
			return Collections.EMPTY_LIST;
		}
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(prevExecPath.get(),
				new PathFilter() {

					@Override
					public boolean accept(Path path) {
						return path.getName().startsWith(
								StormOrderMetaConfigs.HIVE_ORDER_NOEND_PREFIX)
								&& cantianTopic(path.getName(), topics);
					}
				});
		List<Path> paths = new ArrayList<Path>();
		for (FileStatus fileStatus2 : fileStatus) {
			paths.add(fileStatus2.getPath());
		}

		return paths;

	}

	private static boolean cantianTopic(String path, Set<String> topics) {
		for (String topic : topics) {
			if (path.indexOf(topic) != -1) {
				return true;
			}
		}
		return false;
	}

	private static class OffsetFileFilter implements PathFilter {
		@Override
		public boolean accept(Path arg0) {
			return arg0.getName().startsWith(
					StormOrderMetaConfigs.CAMUS_OFFSET_PREFIX);
		}
	}
}
