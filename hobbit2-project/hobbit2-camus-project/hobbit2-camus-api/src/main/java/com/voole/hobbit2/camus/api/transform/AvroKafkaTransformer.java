package com.voole.hobbit2.camus.api.transform;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.base.Optional;

public class AvroKafkaTransformer implements
		ITransformer<byte[], SpecificRecordBase> {

	private final SpecificDatumReader<?> reader;

	@SuppressWarnings("rawtypes")
	public AvroKafkaTransformer(Schema schema) {
		reader = new SpecificDatumReader(schema);
	}

	@Override
	public Optional<SpecificRecordBase> transform(byte[] bytes)
			throws TransformException {
		BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes,
				null);
		try {
			SpecificRecordBase record = (SpecificRecordBase) reader.read(null,
					binaryDecoder);
			if (record != null) {
				return Optional.of(record);
			} else {
				return Optional.absent();
			}
		} catch (IOException e) {
			throw new TransformException(e);
		}

	}
}
