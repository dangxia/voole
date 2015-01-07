/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.test.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.reflect.Manifest;
import scala.reflect.ManifestFactory;
import backtype.storm.serialization.IKryoDecorator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.KryoSerializer;
import com.twitter.chill.avro.AvroSerializer;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class StromAvroKryoDecorator implements IKryoDecorator {
	private static final Logger log = LoggerFactory
			.getLogger(StromAvroKryoDecorator.class);

	public StromAvroKryoDecorator() {
	}

	@Override
	public void decorate(Kryo k) {
		List<Class<? extends SpecificRecordBase>> list = new ArrayList<Class<? extends SpecificRecordBase>>();
		list.add(com.voole.hobbit2.storm.test.avro.model.AvroModel1.class);
		list.add(com.voole.hobbit2.storm.test.avro.model.AvroModel2.class);
		for (Class<? extends SpecificRecordBase> clazz : list) {
			log.info("avro KryoDecorator Registe: Class-->" + clazz.getName());
			k.register(
					clazz,
					new SwappSerializer(AvroSerializer
							.SpecificRecordBinarySerializer(getManifest(clazz))));
		}
		IKryoRegistrar kryoRegistrar = KryoSerializer.registerAll();
		kryoRegistrar.apply(k);
	}

	protected <T> Manifest<T> getManifest(Class<T> clazz) {
		return ManifestFactory.classType(clazz);
	}

	public static class SwappSerializer<T extends SpecificRecordBase> extends
			Serializer<T> {

		private final Serializer<T> serializer;

		public SwappSerializer(Serializer<T> serializer) {
			this.serializer = serializer;
		}

		@Override
		public void write(Kryo kryo, Output output, T object) {
			System.out.println("--------------------write--------------");
			getSerializer().write(kryo, output, object);
		}

		@Override
		public T read(Kryo kryo, Input input, Class<T> type) {
			System.out.println("--------------------read--------------");
			return getSerializer().read(kryo, input, type);
		}

		public Serializer<T> getSerializer() {
			return serializer;
		}

	}

}
