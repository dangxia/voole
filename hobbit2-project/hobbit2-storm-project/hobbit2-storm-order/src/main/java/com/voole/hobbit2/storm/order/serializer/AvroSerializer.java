/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import backtype.storm.serialization.IKryoDecorator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class AvroSerializer<T extends SpecificRecordBase> extends Serializer<T> {
	private final DatumWriter<SpecificRecordBase> writer;
	private final OutPutWapper outPutWapper;
	private final Encoder encoder;

	private final DatumReader<SpecificRecordBase> reader;
	private final InputStreamWapper inPutWapper;
	private final Decoder decoder;

	public AvroSerializer() {
		this(StormOrderMetaConfigs.getOrderUnionSchema());
	}

	public AvroSerializer(Schema schema) {
		IKryoDecorator t;
		
		writer = new SpecificDatumWriter<SpecificRecordBase>(schema);
		outPutWapper = new OutPutWapper();
		encoder = EncoderFactory.get().binaryEncoder(outPutWapper, null);

		reader = new SpecificDatumReader<SpecificRecordBase>(schema);
		inPutWapper = new InputStreamWapper();
		decoder = DecoderFactory.get().binaryDecoder(inPutWapper, null);
	}

	@Override
	public synchronized void write(Kryo kryo, Output output, T object) {
		outPutWapper.setIndeed(output);
		try {
			writer.write(object, encoder);
			encoder.flush();
		} catch (IOException e) {
			Throwables.propagate(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized T read(Kryo kryo, Input input, Class<T> type) {
		inPutWapper.setIndeed(input);
		try {
			return (T) reader.read(null, decoder);
		} catch (IOException e) {
			Throwables.propagate(e);
		}

		return null;

	}

	public class OutPutWapper extends OutputStream {
		private OutputStream indeed;

		public void write(int b) throws IOException {
			getIndeed().write(b);
		}

		public void write(byte b[]) throws IOException {
			getIndeed().write(b);
		}

		public void write(byte b[], int off, int len) throws IOException {
			getIndeed().write(b, off, len);
		}

		public void flush() throws IOException {
			getIndeed().flush();
		}

		public void close() throws IOException {
			getIndeed().close();
		}

		public OutputStream getIndeed() {
			return indeed;
		}

		public void setIndeed(OutputStream indeed) {
			this.indeed = indeed;
		}

	}

	public class InputStreamWapper extends InputStream {
		private InputStream indeed;

		public int read() throws IOException {
			return getIndeed().read();
		}

		public int read(byte b[]) throws IOException {
			return getIndeed().read(b);
		}

		public int read(byte b[], int off, int len) throws IOException {
			return getIndeed().read(b, off, len);
		}

		public long skip(long n) throws IOException {
			return getIndeed().skip(n);
		}

		public int available() throws IOException {
			return getIndeed().available();
		}

		public void close() throws IOException {
			getIndeed().close();
		}

		public synchronized void mark(int readlimit) {
			getIndeed().mark(readlimit);
		}

		public synchronized void reset() throws IOException {
			getIndeed().reset();
		}

		public boolean markSupported() {
			return getIndeed().markSupported();
		}

		public InputStream getIndeed() {
			return indeed;
		}

		public void setIndeed(InputStream indeed) {
			this.indeed = indeed;
		}

	}

}
