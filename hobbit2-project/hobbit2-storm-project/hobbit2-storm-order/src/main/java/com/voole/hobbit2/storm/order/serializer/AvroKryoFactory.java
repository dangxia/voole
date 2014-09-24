/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.serializer;

import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;

import backtype.storm.Config;
import backtype.storm.serialization.DefaultKryoFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class AvroKryoFactory extends DefaultKryoFactory {
	public static class KryoSerializableDefault2 extends
			KryoSerializableDefault {
		@SuppressWarnings("rawtypes")
		private final Serializer serializer;

		public KryoSerializableDefault2() {
			serializer = new AvroSerializer<SpecificRecordBase>();
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Serializer getDefaultSerializer(Class type) {
			if (SpecificRecordBase.class.isAssignableFrom(type)) {
				return serializer;
			}
			return super.getDefaultSerializer(type);
		}

	}

	@Override
	public Kryo getKryo(@SuppressWarnings("rawtypes") Map conf) {
		KryoSerializableDefault2 k = new KryoSerializableDefault2();
		k.setRegistrationRequired(!((Boolean) conf
				.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
		k.setReferences(false);
		return k;
	}
}
