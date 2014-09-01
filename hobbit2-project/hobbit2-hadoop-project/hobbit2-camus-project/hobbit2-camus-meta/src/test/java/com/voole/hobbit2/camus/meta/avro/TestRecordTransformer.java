/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年9月1日
 */
public class TestRecordTransformer implements KafkaTransformer<TestRecord> {

	@Override
	public TestRecord transform(byte[] bytes) throws KafkaTransformException {
		DatumReader<TestRecord> userDatumReader = new SpecificDatumReader<TestRecord>(
				TestRecord.class);
		DataFileReader<TestRecord> dataFileReader = null;
		try {
			dataFileReader = new DataFileReader<TestRecord>(
					new SeekableByteArrayInput(bytes), userDatumReader);
			if (dataFileReader.hasNext()) {
				return dataFileReader.next();
			}
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (dataFileReader != null) {
				try {
					dataFileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	@Override
	public TestRecord transform(String str) throws KafkaTransformException {
		return transform(str.getBytes());
	}

}
