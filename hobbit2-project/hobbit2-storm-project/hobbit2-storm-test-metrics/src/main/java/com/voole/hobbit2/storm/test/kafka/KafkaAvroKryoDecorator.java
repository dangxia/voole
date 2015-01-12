/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.test.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.reflect.Manifest;
import scala.reflect.ManifestFactory;
import backtype.storm.serialization.IKryoDecorator;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.KryoSerializer;
import com.twitter.chill.avro.AvroSerializer;
import com.voole.hobbit2.storm.test.kafka.avro.PartitionInfo;
import com.voole.hobbit2.storm.test.kafka.avro.PlayAlive;
import com.voole.hobbit2.storm.test.kafka.avro.PlayBgn;
import com.voole.hobbit2.storm.test.kafka.avro.PlayEnd;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class KafkaAvroKryoDecorator implements IKryoDecorator {
	private static final Logger log = LoggerFactory
			.getLogger(KafkaAvroKryoDecorator.class);

	public KafkaAvroKryoDecorator() {
	}

	@Override
	public void decorate(Kryo k) {
		List<Class<? extends SpecificRecordBase>> list = new ArrayList<Class<? extends SpecificRecordBase>>();
		list.add(PartitionInfo.class);
		list.add(PlayBgn.class);
		list.add(PlayEnd.class);
		list.add(PlayAlive.class);
		for (Class<? extends SpecificRecordBase> clazz : list) {
			log.info("avro KryoDecorator Registe: Class-->" + clazz.getName());
			k.register(clazz, AvroSerializer
					.SpecificRecordBinarySerializer(getManifest(clazz)));
		}
		IKryoRegistrar kryoRegistrar = KryoSerializer.registerAll();
		kryoRegistrar.apply(k);
	}

	protected <T> Manifest<T> getManifest(Class<T> clazz) {
		return ManifestFactory.classType(clazz);
	}

}
