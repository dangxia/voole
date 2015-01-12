package com.voole.avro.usage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.voole.avro.usage.model.AvroModelVersion1;

public class Test2 {
	public static void main(String[] args) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out,
				null);
		binaryEncoder.writeLong(1);

		SpecificDatumWriter<AvroModelVersion1> writer = new SpecificDatumWriter<AvroModelVersion1>(
				AvroModelVersion1.getClassSchema());
		AvroModelVersion1 datum = new AvroModelVersion1();
		datum.setName("name-teset");
		datum.setNum(-1l);

		writer.write(datum, binaryEncoder);

		binaryEncoder.flush();

		byte[] bytes = out.toByteArray();

		out.close();

		BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes,
				null);

		SpecificDatumReader<AvroModelVersion1> reader = new SpecificDatumReader<AvroModelVersion1>(
				AvroModelVersion1.getClassSchema());

		System.out.println("version:" + binaryDecoder.readLong());

		AvroModelVersion1 record = reader.read(null, binaryDecoder);

		System.out.println(record);
	}
}
