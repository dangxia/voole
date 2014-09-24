/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.kryodecorator;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;

import scala.reflect.Manifest;
import scala.reflect.ManifestFactory;
import backtype.storm.serialization.IKryoDecorator;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.KryoSerializer;
import com.twitter.chill.avro.AvroSerializer;
import com.voole.hobbit2.camus.OrderTopicsUtils;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class TestKryoDecorator implements IKryoDecorator {

	private final List<Class<? extends SpecificRecordBase>> list;

	public TestKryoDecorator() {
		list = new ArrayList<Class<? extends SpecificRecordBase>>();
		list.addAll(OrderTopicsUtils.topicBiClazz.values());
		list.addAll(OrderTopicsUtils.topicBiSrvClazz.values());
	}

	@Override
	public void decorate(Kryo k) {
		for (Class<? extends SpecificRecordBase> clazz : list) {
			k.register(clazz,
					AvroSerializer.SpecificRecordSerializer(getManifest(clazz)));
		}
		IKryoRegistrar kryoRegistrar = KryoSerializer.registerAll();
		kryoRegistrar.apply(k);
	}

	protected <T> Manifest<T> getManifest(Class<T> clazz) {
		return ManifestFactory.classType(OrderPlayEndReqV2.class);
	}

}
