/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.voole.hobbit.util.AvroUtils;

/**
 * @author XuehuiHe
 * @date 2014年7月16日
 */
public class ReadAvro {
	public static void main(String[] args) throws IOException {
		File file = new File(
				"/tmp/kafka/t_playalive_v2/hourly/2014/07/16/19/t_playalive_v2.4.2.4023.13952518.avro");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				AvroUtils.getKafkaTopicSchema("t_playalive_v2"));
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}
}
