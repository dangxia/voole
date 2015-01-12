package com.voole.avro.usage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class SpecificRecordBinarySerializer {
	private final Map<Long, SpecificDatumWriter<SpecificRecordBase>> cacheDatumWriters;

	private final ThreadLocal<BinaryEncoderProxy> localBinaryEncoderProxy;

	public SpecificRecordBinarySerializer(Map<Long, Schema> config) {
		cacheDatumWriters = new HashMap<Long, SpecificDatumWriter<SpecificRecordBase>>();
		localBinaryEncoderProxy = new ThreadLocal<BinaryEncoderProxy>();
		for (Entry<Long, Schema> entry : config.entrySet()) {
			Long version = entry.getKey();
			Schema schema = entry.getValue();

			Preconditions.checkNotNull(version);
			Preconditions.checkNotNull(schema);

			cacheDatumWriters.put(version,
					new SpecificDatumWriter<SpecificRecordBase>(schema));
		}
	}

	public SpecificRecordBinarySerializer(Long version, Schema schema) {
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
	 * @param version
	 * @param datum
	 * @return
	 * @throws IOException
	 */
	public byte[] serialize(Long version, SpecificRecordBase datum)
			throws IOException {
		Preconditions.checkNotNull(version);
		Preconditions.checkNotNull(datum);
		SpecificDatumWriter<SpecificRecordBase> datumWriter = cacheDatumWriters
				.get(version);
		Preconditions.checkNotNull(datumWriter);

		BinaryEncoderProxy proxy = getBinaryEncoderProxy();
		proxy.getBinaryEncoder().writeLong(version);
		datumWriter.write(datum, proxy.getBinaryEncoder());
		proxy.getBinaryEncoder().flush();

		return proxy.getOut().toByteArray();

	}

	public BinaryEncoderProxy getBinaryEncoderProxy() {
		BinaryEncoderProxy proxy = localBinaryEncoderProxy.get();
		if (proxy == null) {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			proxy = new BinaryEncoderProxy(out);
			localBinaryEncoderProxy.set(proxy);
		}
		proxy.getOut().reset();

		return proxy;
	}

	class BinaryEncoderProxy {
		private final ByteArrayOutputStream out;
		private final BinaryEncoder binaryEncoder;

		public BinaryEncoderProxy(ByteArrayOutputStream out) {
			this.out = out;
			this.binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
		}

		public ByteArrayOutputStream getOut() {
			return out;
		}

		public BinaryEncoder getBinaryEncoder() {
			return binaryEncoder;
		}
	}
}
