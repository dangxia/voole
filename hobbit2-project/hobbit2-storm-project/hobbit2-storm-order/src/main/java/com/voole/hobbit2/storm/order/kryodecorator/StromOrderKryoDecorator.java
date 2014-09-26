/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.kryodecorator;

import java.util.HashSet;
import java.util.Set;

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
import com.voole.hobbit2.camus.OrderTopicsUtils;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class StromOrderKryoDecorator implements IKryoDecorator {
	private static final Logger log = LoggerFactory
			.getLogger(StromOrderKryoDecorator.class);
	private final Set<Class<? extends SpecificRecordBase>> list;

	public StromOrderKryoDecorator() {
		list = new HashSet<Class<? extends SpecificRecordBase>>();
		for (String topic : StormOrderMetaConfigs.getWhiteTopics()) {
			list.add(OrderTopicsUtils.topicDryClazz.get(topic));
			list.add(OrderTopicsUtils.topicBiClazz.get(topic));
			list.add(OrderTopicsUtils.topicBiSrvClazz.get(topic));
		}
	}

	@Override
	public void decorate(Kryo k) {
		for (Class<? extends SpecificRecordBase> clazz : list) {
			log.info("avro KryoDecorator Registe: Class-->" + clazz.getName());
			k.register(clazz,
					AvroSerializer.SpecificRecordSerializer(getManifest(clazz)));
		}
		IKryoRegistrar kryoRegistrar = KryoSerializer.registerAll();
		kryoRegistrar.apply(k);
	}

	protected <T> Manifest<T> getManifest(Class<T> clazz) {
		return ManifestFactory.classType(clazz);
	}

}
