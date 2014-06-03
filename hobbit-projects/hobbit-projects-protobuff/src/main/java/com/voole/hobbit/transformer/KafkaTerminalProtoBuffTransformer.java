/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessage.Builder;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;

/**
 * @author XuehuiHe
 * @date 2014年5月30日
 */
public class KafkaTerminalProtoBuffTransformer<T extends GeneratedMessage>
		implements KafkaProtoBuffTransformer<T> {
	private final Class<T> clazz;
	private final List<FieldDescriptor> fieldDescriptors;
	private final int count;

	private int repeatedCount;
	private int repeatIndex;
	private List<FieldDescriptor> repeatedfieldDescriptors;
	private FieldDescriptor repeatedFieldDescriptor;

	public KafkaTerminalProtoBuffTransformer(Class<T> clazz)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException, ClassNotFoundException {
		this.clazz = clazz;
		this.fieldDescriptors = new ArrayList<FieldDescriptor>();
		int repeatIndex = 0;
		for (FieldDescriptor fieldDescriptor : getDescriptor(clazz).getFields()) {
			if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
				this.repeatIndex = repeatIndex;
				this.repeatedFieldDescriptor = fieldDescriptor;
				this.repeatedfieldDescriptors = fieldDescriptor
						.getMessageType().getFields();
				this.repeatedCount = this.repeatedfieldDescriptors.size();
			} else {
				this.fieldDescriptors.add(fieldDescriptor);
			}
			repeatIndex++;
		}
		this.count = this.fieldDescriptors.size();

	}

	public Builder<?> newBuilder() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		return newBuilder(clazz);
	}

	public static Descriptor getDescriptor(Class<?> clazz)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		return (Descriptor) clazz.getMethod("getDescriptor").invoke(null);
	}

	public static Builder<?> newBuilder(Class<?> clazz)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		return (Builder<?>) clazz.getMethod("newBuilder").invoke(null);
	}

	public int getRepeatTimes(int length) {
		if (repeatedFieldDescriptor == null) {
			if (length == this.count) {
				return 0;
			}
		} else {
			int repeatedTotal = length - this.count;
			if (repeatedTotal % repeatedCount == 0) {
				return repeatedTotal / repeatedCount;
			}
		}
		return -1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T transform(byte[] bytes) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String msg = new String(bytes);
		String[] items = msg.split("\\|");
		int repeatTimes = getRepeatTimes(items.length);
		if (repeatTimes == -1) {
			throw new RuntimeException(msg);
		}
		List<String[]> repeatedData = null;
		if (repeatTimes != 0) {
			repeatedData = new LinkedList<String[]>();
			int from = repeatIndex;
			for (int i = 0; i < repeatTimes; i++) {
				repeatedData.add(Arrays.copyOfRange(items, from, from
						+ repeatedCount));
				from += repeatedCount;
			}

			String[] newItems = new String[count];
			String[] headItems = null;
			if (repeatIndex > 0) {
				headItems = Arrays.copyOfRange(items, 0, repeatIndex);
				System.arraycopy(headItems, 0, newItems, 0, headItems.length);
			}
			if (count > repeatIndex) {
				String[] tailItems = Arrays.copyOfRange(items, repeatIndex
						+ repeatTimes * repeatedCount, items.length);
				System.arraycopy(tailItems, 0, newItems, headItems == null ? 0
						: headItems.length, tailItems.length);
			}
			items = newItems;
		}
		Builder<?> builder = newBuilder();
		for (int i = 0; i < items.length; i++) {
			String item = items[i];
			set(builder, fieldDescriptors.get(i), item);
		}
		if (repeatedData != null) {
			for (String[] repeatedItem : repeatedData) {
				Builder<?> repeatedBuilder = (Builder<?>) builder
						.newBuilderForField(repeatedFieldDescriptor);
				for (int i = 0; i < repeatedItem.length; i++) {
					String item = repeatedItem[i];
					set(repeatedBuilder, repeatedfieldDescriptors.get(i), item);
				}
				builder.addRepeatedField(repeatedFieldDescriptor,
						repeatedBuilder.build());
			}
		}

		return (T) builder.build();
	}

	/**
	 * @param builder
	 * @param fieldDescriptor
	 * @param item
	 */
	private void set(Builder<?> builder, FieldDescriptor fieldDescriptor,
			String item) {
		JavaType type = fieldDescriptor.getJavaType();
		Object v = null;
		switch (type) {
		case INT:
			v = Integer.parseInt(item);
			break;
		case LONG:
			v = Long.parseLong(item);
			break;
		case FLOAT:
			v = Float.parseFloat(item);
			break;
		case DOUBLE:
			v = Double.parseDouble(item);
			break;
		case BOOLEAN:
			v = Boolean.parseBoolean(item);
			break;
		default:
			v = item;
			break;
		}
		builder.setField(fieldDescriptor, v);

	}

	public static void main(String[] args) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		String str = "7258640872482230420|1401027747|1155085|1401179826|3|15318|0|0|1564913|0|5|2188553949|1|15366|2313|1018102|452|1466050|3148197|3024|4|2205331165|1|15295|2341|443367|456|1423117|2508171|3216|7|2188553949|1|15231|2242|476209|451|1242039|3148801|2248|6|2205331165|1|15407|2261|1018102|416|874970|2562178|2100|5|2356326109|1|0|0|0|0|0|0|0|57160|0";
		// str =
		// "7258640872482230420|1401027747|1155085|1401179826|3|15318|0|0|1564913|0|5|2188553949|1|15366|2313|1018102|452|1466050|3148197|3024|4|2205331165";
		KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV3> transformer = new KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV3>(
				OrderPlayAliveReqV3.class);
		OrderPlayAliveReqV3 t = transformer.transform(str.getBytes());
		System.out.println(getInfo(t));
	}

	public static String getInfo(GeneratedMessage t) {
		StringBuffer sb = new StringBuffer();
		boolean isBegined = false;
		List<FieldDescriptor> fds = t.getDescriptorForType().getFields();
		for (FieldDescriptor fd : fds) {
			if (fd.getJavaType() == JavaType.MESSAGE) {
				for (Object o : (Collection<?>) t.getField(fd)) {
					if (isBegined) {
						sb.append("|");
					} else {
						isBegined = true;
					}
					String info = getInfo((GeneratedMessage) o);
					sb.append(info);
				}
			} else {
				if (isBegined) {
					sb.append("|");
				} else {
					isBegined = true;
				}
				sb.append(t.getField(fd));
			}
		}
		return sb.toString();
	}

	public static void main2(String[] args) {
		Descriptor descriptor = OrderPlayAliveReqV2.getDescriptor();
		System.out.println(descriptor.getContainingType().getName());
		for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
			String str = "index:" + fieldDescriptor.getIndex() + "\tfullname:"
					+ fieldDescriptor.getFullName() + "\tname:"
					+ fieldDescriptor.getName() + "\tnum:"
					+ fieldDescriptor.getNumber()
					+ fieldDescriptor.getJavaType();

			if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
				str += fieldDescriptor.getMessageType().getName();
			}
			// + descriptor.toProto().
			System.out.println(str);
		}

	}
}
