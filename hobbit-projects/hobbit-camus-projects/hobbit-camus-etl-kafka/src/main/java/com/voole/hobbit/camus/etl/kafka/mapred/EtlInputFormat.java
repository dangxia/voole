package com.voole.hobbit.camus.etl.kafka.mapred;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.etl.kafka.CamusConfigs;
import com.voole.hobbit.camus.etl.kafka.CamusJob;
import com.voole.hobbit.camus.etl.kafka.common.EtlKey;
import com.voole.hobbit.camus.etl.kafka.common.EtlRequest;
import com.voole.hobbit.camus.etl.kafka.common.KafkaMetaUtils;
import com.voole.hobbit.camus.etl.kafka.common.LeaderInfo;
import com.voole.hobbit.camus.etl.kafka.util.SequenceFileUtils;

/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, CamusWrapper<?>> {
	private final Logger log = Logger.getLogger(getClass());

	@Override
	public RecordReader<EtlKey, CamusWrapper<?>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new EtlRecordReader(split, context);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		CamusJob.startTiming("getSplits");
		ArrayList<EtlRequest> finalRequests;
		HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();
		try {
			// Get Filted Metadata for all topics
			List<TopicMetadata> topicMetadataList = KafkaMetaUtils
					.getKafkaMetadata(context);
			for (TopicMetadata topicMetadata : topicMetadataList) {
				for (PartitionMetadata partitionMetadata : topicMetadata
						.partitionsMetadata()) {
					if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
						log.info("Skipping the creation of ETL request for Topic : "
								+ topicMetadata.topic()
								+ " and Partition : "
								+ partitionMetadata.partitionId()
								+ " Exception : "
								+ ErrorMapping.exceptionFor(partitionMetadata
										.errorCode()));
						continue;
					} else {

						LeaderInfo leader = new LeaderInfo(new URI("tcp://"
								+ partitionMetadata.leader()
										.getConnectionString()),
								partitionMetadata.leader().id());
						ArrayList<TopicAndPartition> topicAndPartitions = null;
						if (offsetRequestInfo.containsKey(leader)) {
							topicAndPartitions = offsetRequestInfo.get(leader);
						} else {
							topicAndPartitions = new ArrayList<TopicAndPartition>();
							offsetRequestInfo.put(leader, topicAndPartitions);
						}
						topicAndPartitions.add(new TopicAndPartition(
								topicMetadata.topic(), partitionMetadata
										.partitionId()));
					}
				}

			}
		} catch (Exception e) {
			log.error(
					"Unable to pull requests from Kafka brokers. Exiting the program",
					e);
			return null;
		}
		// Get the latest offsets and generate the EtlRequests

		finalRequests = KafkaMetaUtils.fetchLatestOffsetAndCreateEtlRequests(
				context, offsetRequestInfo);

		writeRequests(finalRequests, context);
		Map<EtlRequest, EtlKey> offsetKeys = getPreviousOffsets(
				FileInputFormat.getInputPaths(context), context);
		Set<String> moveLatest = getMoveToLatestTopicsSet(context);
		for (EtlRequest request : finalRequests) {
			if (moveLatest.contains(request.getTopic())
					|| moveLatest.contains("all")) {
				offsetKeys.put(
						request,
						new EtlKey(request.getTopic(), request.getLeaderId(),
								request.getPartition(), 0, request
										.getLastOffset()));
			}

			EtlKey key = offsetKeys.get(request);

			if (key != null) {
				request.setOffset(key.getOffset());
			}

			if (request.getEarliestOffset() > request.getOffset()
					|| request.getOffset() > request.getLastOffset()) {
				if (request.getEarliestOffset() > request.getOffset()) {
					log.error("The earliest offset was found to be more than the current offset");
					log.error("Moving to the earliest offset available");
				} else {
					log.error("The current offset was found to be more than the latest offset");
					log.error("Moving to the earliest offset available");
				}
				request.setOffset(request.getEarliestOffset());
				offsetKeys.put(
						request,
						new EtlKey(request.getTopic(), request.getLeaderId(),
								request.getPartition(), 0, request
										.getLastOffset()));
			}
			log.info(request);
		}

		writePrevious(offsetKeys.values(), context);

		CamusJob.stopTiming("getSplits");
		CamusJob.startTiming("hadoop");
		CamusJob.setTime("hadoop_start");
		return allocateWork(finalRequests, context);
	}

	private Set<String> getMoveToLatestTopicsSet(JobContext context) {
		Set<String> topics = new HashSet<String>();

		String[] arr = CamusConfigs.getMoveToLatestTopics(context);

		if (arr != null) {
			for (String topic : arr) {
				topics.add(topic);
			}
		}

		return topics;
	}

	private List<InputSplit> allocateWork(List<EtlRequest> requests,
			JobContext context) throws IOException {
		int numTasks = CamusConfigs.getMapredMapTasks(context);

		// Reverse sort by size
		Collections.sort(requests, new Comparator<EtlRequest>() {
			@Override
			public int compare(EtlRequest o1, EtlRequest o2) {
				if (o2.estimateDataSize() == o1.estimateDataSize()) {
					return 0;
				}
				if (o2.estimateDataSize() < o1.estimateDataSize()) {
					return -1;
				} else {
					return 1;
				}
			}
		});

		List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

		for (int i = 0; i < numTasks; i++) {
			EtlSplit split = new EtlSplit();

			if (requests.size() > 0) {
				split.addRequest(requests.get(0));
				kafkaETLSplits.add(split);
				requests.remove(0);
			}
		}

		for (EtlRequest r : requests) {
			getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
		}

		return kafkaETLSplits;
	}

	private EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits)
			throws IOException {
		EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

		for (int i = 1; i < kafkaETLSplits.size(); i++) {
			EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
			if ((smallest.getLength() == challenger.getLength() && smallest
					.getNumRequests() > challenger.getNumRequests())
					|| smallest.getLength() > challenger.getLength()) {
				smallest = challenger;
			}
		}

		return smallest;
	}

	private void writePrevious(Collection<EtlKey> missedKeys, JobContext context)
			throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, CamusConfigs.OFFSET_PREFIX + "-previous");
		SequenceFile.Writer writer = SequenceFileUtils.createWriter(fs,
				context.getConfiguration(), output, EtlKey.class,
				NullWritable.class);

		for (EtlKey key : missedKeys) {
			writer.append(key, NullWritable.get());
		}

		writer.close();
	}

	private void writeRequests(List<EtlRequest> requests, JobContext context)
			throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, CamusConfigs.REQUESTS_FILE);

		SequenceFile.Writer writer = SequenceFileUtils.createWriter(fs,
				context.getConfiguration(), output, EtlRequest.class,
				NullWritable.class);

		for (EtlRequest r : requests) {
			writer.append(r, NullWritable.get());
		}
		writer.close();
	}

	private Map<EtlRequest, EtlKey> getPreviousOffsets(Path[] inputs,
			JobContext context) throws IOException {
		Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<EtlRequest, EtlKey>();
		for (Path input : inputs) {
			FileSystem fs = input.getFileSystem(context.getConfiguration());
			for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
				log.info("previous offset file:" + f.getPath().toString());

				SequenceFile.Reader reader = SequenceFileUtils.createReader(fs,
						f.getPath(), context.getConfiguration());
				EtlKey key = new EtlKey();
				while (reader.next(key, NullWritable.get())) {
					EtlRequest request = new EtlRequest(context,
							key.getTopic(), key.getLeaderId(),
							key.getPartition());
					if (offsetKeysMap.containsKey(request)) {

						EtlKey oldKey = offsetKeysMap.get(request);
						if (oldKey.getOffset() < key.getOffset()) {
							offsetKeysMap.put(request, key);
						}
					} else {
						offsetKeysMap.put(request, key);
					}
					key = new EtlKey();
				}
				reader.close();
			}
		}
		return offsetKeysMap;
	}

	private class OffsetFileFilter implements PathFilter {

		@Override
		public boolean accept(Path arg0) {
			return arg0.getName().startsWith(CamusConfigs.OFFSET_PREFIX);
		}
	}
}
