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
import com.voole.hobbit.util.TransformerUtil;

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
		String[] items = msg.split("\\t");
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
		try {
			Object v = TransformerUtil.get(fieldDescriptor.getJavaType(), item);
			builder.setField(fieldDescriptor, v);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		String str = "13925774514353362612	1401781295	365829	1401874072	40	43636	1	873897440	810488	0	10	703741367	1	1044	68605	32842	173	125377	228250	43016	16	737295799	1	1040	65050	0	0	107562	200351	32420	14	4205515121	1	971	63480	12212	69	93053	176642	38418	8	770850231	1	1046	69887	16421	96	77893	169529	56359	13	1515138363	1	167	129469	82105	1199	64921	93694	93532	25	703741367	1	0	0	0	0	0	0	0	34	737295799	1	0	0	0	0	0	0	0	38	770850231	1	0	0	0	0	0	0	0	46	4205515121	1	0	0	0	0	0	0	0	48	1515138363	1	0	0	0	0	0	0	0	49	1617807";
		OrderPlayAliveReqV3 t = (OrderPlayAliveReqV3) KafkaProtoBuffTransformerFactory
				.newTransformer("t_playalive_v3").transform(str.getBytes());
		System.out.println(t.getAliveTick() * 1000);
		System.out.println(System.currentTimeMillis());
	}

	public static void main4(String[] args) throws IllegalAccessException,
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
