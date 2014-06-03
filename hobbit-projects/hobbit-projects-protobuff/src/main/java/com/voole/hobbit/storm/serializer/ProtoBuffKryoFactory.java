/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.serializer;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.serialization.DefaultKryoFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.google.protobuf.GeneratedMessage;

/**
 * @author XuehuiHe
 * @date 2014年5月30日
 */
public class ProtoBuffKryoFactory extends DefaultKryoFactory {
	public static class KryoSerializableDefault2 extends
			KryoSerializableDefault {
		private final Serializer serializer;

		public KryoSerializableDefault2() {
			serializer = new ProtoBuffSerializer<GeneratedMessage>();
		}

		@Override
		public Serializer getDefaultSerializer(Class type) {
			if (GeneratedMessage.class.isAssignableFrom(type)) {
				return serializer;
			}
			return super.getDefaultSerializer(type);
		}

	}

	@Override
	public Kryo getKryo(Map conf) {
		KryoSerializableDefault2 k = new KryoSerializableDefault2();
		k.setRegistrationRequired(!((Boolean) conf
				.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
		k.setReferences(false);
		return k;
	}
}
