package com.voole.hobbit.camus.etl.kafka.coders;

import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.camus.coders.MessageDecoder;

public interface MessageDecoderFactory {

	public MessageDecoder<?, ?> createMessageDecoder(JobContext context,
			String topicName);

}
