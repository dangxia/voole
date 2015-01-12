package com.voole.avro.usage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class SpecificRecordBinaryDeserializer {

	private final Map<Long, SpecificDatumReader<SpecificRecordBase>> cacheDatumReaders;
	private final ThreadLocal<BinaryDecoder> localBinaryDecoder;

	public SpecificRecordBinaryDeserializer(Map<Long, Schema> config) {
		cacheDatumReaders = new HashMap<Long, SpecificDatumReader<SpecificRecordBase>>();
		localBinaryDecoder = new ThreadLocal<BinaryDecoder>();
		for (Entry<Long, Schema> entry : config.entrySet()) {
			Long version = entry.getKey();
			Schema schema = entry.getValue();

			Preconditions.checkNotNull(version);
			Preconditions.checkNotNull(schema);

			cacheDatumReaders.put(version,
					new SpecificDatumReader<SpecificRecordBase>(schema));
		}
	}

	public SpecificRecordBinaryDeserializer(Long version, Schema schema) {
		this(createConfig(version, schema));
	}

	private static Map<Long, Schema> createConfig(Long version, Schema schema) {
		Map<Long, Schema> config = Maps.newHashMap();
		config.put(version, schema);
		return config;
	}

	/**
	 * thread safe
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */

	public Tuple deserialize(byte[] bytes) throws IOException {
		BinaryDecoder binaryDecoder = getBinaryDecoder(bytes);
		Long version = binaryDecoder.readLong();
		SpecificDatumReader<SpecificRecordBase> datumReader = cacheDatumReaders
				.get(version);
		Preconditions.checkNotNull(datumReader);
		SpecificRecordBase record = datumReader.read(null, binaryDecoder);

		Tuple tuple = new Tuple();
		tuple.setVersion(version);
		tuple.setRecord(record);
		return tuple;

	}

	private BinaryDecoder getBinaryDecoder(byte[] bytes) {
		BinaryDecoder binaryDecoder = localBinaryDecoder.get();
		if (binaryDecoder == null) {
			binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
			localBinaryDecoder.set(binaryDecoder);
		} else {
			DecoderFactory.get().binaryDecoder(bytes, binaryDecoder);
		}

		return binaryDecoder;
	}

	public static class Tuple {
		private Long version;
		private SpecificRecordBase record;

		public Tuple() {
		}

		public Long getVersion() {
			return version;
		}

		public void setVersion(Long version) {
			this.version = version;
		}

		public SpecificRecordBase getRecord() {
			return record;
		}

		public void setRecord(SpecificRecordBase record) {
			this.record = record;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("version", getVersion())
					.add("record", getRecord()).toString();
		}

	}

}
