package com.voole.avro.usage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import com.voole.avro.usage.model.AvroModelVersion1;
import com.voole.avro.usage.model.AvroModelVersion2;

public class Test3 {
	public static void main(String[] args) throws IOException {
		final AvroModelVersion1 v1 = new AvroModelVersion1();
		v1.setName("this is version 1");
		v1.setNum(100l);

		final AvroModelVersion2 v2 = new AvroModelVersion2();
		v2.setName("this is version 2");
		v2.setNum(-1l);
		v2.setTotal(1000l);

		Map<Long, Schema> config = new HashMap<Long, Schema>();
		config.put(1l, AvroModelVersion1.getClassSchema());
		config.put(2l, AvroModelVersion2.getClassSchema());

		final SpecificRecordBinarySerializer serializer = new SpecificRecordBinarySerializer(
				config);
		final SpecificRecordBinaryDeserializer deserializer = new SpecificRecordBinaryDeserializer(
				config);

		for (int i = 0; i < 20; i++) {
			Thread thread = new Thread() {
				@Override
				public void run() {
					try {
						for (int j = 0; j < 5000; j++) {

							System.out.println(deserializer
									.deserialize(serializer.serialize(1L, v1)));
							System.out.println(deserializer
									.deserialize(serializer.serialize(2L, v2)));
						}
					} catch (Exception e) {
					}

				};
			};
			thread.start();
		}

	}
}
