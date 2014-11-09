/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.kryodecorator;

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
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqSrvV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqSrvV3;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqSrvV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqSrvV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class StromOrderKryoDecorator implements IKryoDecorator {
	private static final Logger log = LoggerFactory
			.getLogger(StromOrderKryoDecorator.class);

	public StromOrderKryoDecorator() {
	}

	@Override
	public void decorate(Kryo k) {
		List<Class<? extends SpecificRecordBase>> list = new ArrayList<Class<? extends SpecificRecordBase>>();
		list.add(HiveOrderDetailRecord.class);

		list.add(OrderPlayBgnReqV2.class);
		list.add(OrderPlayBgnReqV3.class);
		list.add(OrderPlayBgnReqSrvV2.class);
		list.add(OrderPlayBgnReqSrvV3.class);
		
		list.add(OrderPlayAliveReqV2.class);
		list.add(OrderPlayAliveReqV3.class);
		list.add(OrderPlayAliveReqSrvV2.class);
		list.add(OrderPlayAliveReqSrvV3.class);
		
		list.add(OrderPlayEndReqV2.class);
		list.add(OrderPlayEndReqV3.class);
		list.add(OrderPlayEndReqSrvV2.class);
		list.add(OrderPlayEndReqSrvV3.class);

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
