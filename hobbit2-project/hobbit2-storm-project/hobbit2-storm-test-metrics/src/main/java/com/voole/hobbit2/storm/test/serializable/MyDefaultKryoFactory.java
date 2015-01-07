package com.voole.hobbit2.storm.test.serializable;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.serialization.IKryoFactory;
import backtype.storm.serialization.SerializableSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;

public class MyDefaultKryoFactory implements IKryoFactory {
	public static interface MyKryoSerializable extends KryoSerializable {

	}

	public static class KryoSerializableDefault extends Kryo {
		boolean _override = false;

		public void overrideDefault(boolean value) {
			_override = value;
		}

		@Override
		public Serializer getDefaultSerializer(Class type) {
			if (MyKryoSerializable.class.isAssignableFrom(type) || !_override) {
				return super.getDefaultSerializer(type);
			} else {
				return new SerializableSerializer();
			}
		}
	}

	@Override
	public Kryo getKryo(Map conf) {
		KryoSerializableDefault k = new KryoSerializableDefault();
		k.setRegistrationRequired(!((Boolean) conf
				.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
		k.setReferences(false);
		return k;
	}

	@Override
	public void preRegister(Kryo k, Map conf) {
	}

	public void postRegister(Kryo k, Map conf) {
		((KryoSerializableDefault) k).overrideDefault(true);
	}

	@Override
	public void postDecorate(Kryo k, Map conf) {
	}
}
