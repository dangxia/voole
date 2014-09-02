/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.voole.hobbit2.camus.meta.CamusMetaConfigs;
import com.voole.hobbit2.camus.meta.mapreduce.CamusInputSplit;
import com.voole.hobbit2.tools.kafka.partition.Broker;

/**
 * @author XuehuiHe
 * @date 2014年9月2日
 */
public class KafkaReduceReader {
	// index of context
	private static Logger log = Logger.getLogger(KafkaReduceReader.class);
	private CamusInputSplit split = null;
	private SimpleConsumer simpleConsumer = null;

	private long beginOffset;
	private long currentOffset;
	private long lastOffset;
	private long currentCount;

	private TaskAttemptContext context;

	private Iterator<MessageAndOffset> messageIter = null;

	private long totalFetchTime = 0;
	private long lastFetchTime = 0;

	private int fetchBufferSize;

	private final Broker broker;

	/**
	 * Construct using the json representation of the kafka request
	 */
	public KafkaReduceReader(TaskAttemptContext context,
			CamusInputSplit split, int clientTimeout, int fetchBufferSize)
			throws Exception {
		this.broker = split.getBrokerAndTopicPartition().getBroker();
		this.fetchBufferSize = fetchBufferSize;
		this.context = context;

		log.info("bufferSize=" + fetchBufferSize);
		log.info("timeout=" + clientTimeout);

		this.split = split;
		beginOffset = split.getOffset();
		currentOffset = split.getOffset();
		lastOffset = split.getLatestOffset();
		currentCount = 0;
		totalFetchTime = 0;

		// read data from queue

		simpleConsumer = new SimpleConsumer(broker.host(), broker.port(),
				clientTimeout, fetchBufferSize, "");
		log.info("Connected to leader " + broker
				+ " beginning reading at offset " + beginOffset
				+ " latest offset=" + lastOffset);
		fetch();
	}

	public boolean hasNext() throws IOException {
		if (messageIter != null && messageIter.hasNext())
			return true;
		else
			return fetch();

	}

	public boolean getNext(CamusKey key, BytesWritable payload,
			BytesWritable pKey) throws IOException {
		if (hasNext()) {
			MessageAndOffset msgAndOffset = messageIter.next();
			Message message = msgAndOffset.message();

			ByteBuffer buf = message.payload();
			int origSize = buf.remaining();
			byte[] bytes = new byte[origSize];
			buf.get(bytes, buf.position(), origSize);
			payload.set(bytes, 0, origSize);

			buf = message.key();
			if (buf != null) {
				origSize = buf.remaining();
				bytes = new byte[origSize];
				buf.get(bytes, buf.position(), origSize);
				pKey.set(bytes, 0, origSize);
			}

			key.clear();
			key.set(msgAndOffset.offset(), message.checksum());
			currentOffset = msgAndOffset.offset(); // increase offset
			currentCount++; // increase count
			return true;
		} else {
			return false;
		}
	}

	public boolean fetch() throws IOException {
		if (currentOffset >= lastOffset) {
			return false;
		}
		long tempTime = System.currentTimeMillis();
		TopicAndPartition topicAndPartition = split
				.getBrokerAndTopicPartition().getTopicAndPartition();
		log.debug("\nAsking for offset : " + (currentOffset + 1));
		PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(
				currentOffset + 1, fetchBufferSize);

		HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
		fetchInfo.put(topicAndPartition, partitionFetchInfo);
		FetchRequest fetchRequest = new FetchRequest(
				CamusMetaConfigs.getKafkaFetchRequestCorrelationid(context),
				CamusMetaConfigs.getKafkaFetchRequestClientId(context),
				CamusMetaConfigs.getKafkaFetchRequestMaxWait(context),
				CamusMetaConfigs.getKafkaFetchRequestMinBytes(context),
				fetchInfo);

		FetchResponse fetchResponse = null;
		try {
			fetchResponse = simpleConsumer.fetch(fetchRequest);
			if (fetchResponse.hasError()) {
				log.warn("Error encountered during a fetch request from Kafka");
				log.warn("Error Code generated : "
						+ fetchResponse.errorCode(split
								.getBrokerAndTopicPartition().getPartition()
								.getTopic(), split.getBrokerAndTopicPartition()
								.getPartition().getPartition()));
				return false;
			} else {
				ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
						split.getBrokerAndTopicPartition().getPartition()
								.getTopic(), split.getBrokerAndTopicPartition()
								.getPartition().getPartition());
				lastFetchTime = (System.currentTimeMillis() - tempTime);
				log.debug("Time taken to fetch : " + (lastFetchTime / 1000)
						+ " seconds");
				log.debug("The size of the ByteBufferMessageSet returned is : "
						+ messageBuffer.sizeInBytes());
				totalFetchTime += lastFetchTime;
				messageIter = messageBuffer.iterator();
				if (!messageIter.hasNext()) {
					System.out
							.println("No more data left to process. Returning false");
					messageIter = null;
					return false;
				}

				return true;
			}
		} catch (Exception e) {
			log.info("Exception generated during fetch");
			e.printStackTrace();
			return false;
		}

	}

	public void close() throws IOException {
		if (simpleConsumer != null) {
			simpleConsumer.close();
		}
	}

	public long getTotal() {
		return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
	}

	public long getReadedNum() {
		return currentOffset - beginOffset;
	}

	public long getCount() {
		return currentCount;
	}

	public long getFetchTime() {
		return lastFetchTime;
	}

	public long getTotalFetchTime() {
		return totalFetchTime;
	}
}
