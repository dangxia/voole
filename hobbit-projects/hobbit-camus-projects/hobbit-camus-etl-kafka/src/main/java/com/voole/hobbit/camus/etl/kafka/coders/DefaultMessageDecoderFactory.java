/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka.coders;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.coders.MessageDecoder;

/**
 * @author XuehuiHe
 * @date 2014年7月16日
 */
public class DefaultMessageDecoderFactory implements MessageDecoderFactory {

	@Override
	public MessageDecoder<?, ?> createMessageDecoder(JobContext context,
			String topicName) {
		return new MessageDecoder<byte[], String>() {
			@Override
			public CamusWrapper<String> decode(byte[] message) {
				try {
					return new CamusWrapper<String>(
							new String(message, "UTF-8"),
							System.currentTimeMillis());
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				return null;
			}
		};
	}

}
