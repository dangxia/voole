package com.voole.hobbit.storm.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.GeneratedMessage;

public class ProtoBuffSerializer<T extends GeneratedMessage> extends
		Serializer<T> {

	@Override
	public void write(Kryo kryo, Output output, T object) {
		try {
			object.writeTo(output);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T read(Kryo kryo, Input input, Class<T> type) {
		try {
			return (T) type.getMethod("parseFrom", InputStream.class).invoke(
					null, input);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
