/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

import com.voole.hobbit.util.AvroUtils;

/**
 * @author XuehuiHe
 * @date 2014年7月10日
 */
public class KafkaTerminalAvroTransformer implements KafkaTransformer {

	private final Schema schema;

	private final List<Field> fields;
	private final int count;

	private Field arrayField;
	private int arrayCount;
	private int arrayIndex;

	private List<Field> arrayItemFields;
	private Schema arrayItemSchema;
	private final Map<Field, Type> fieldTypeCache;

	public KafkaTerminalAvroTransformer(Schema schema) {
		this.fieldTypeCache = new HashMap<Schema.Field, Schema.Type>();
		this.schema = schema;
		List<Field> fields = schema.getFields();
		this.fields = new ArrayList<Field>();
		int repeatIndex = 0;
		for (Field field : fields) {
			if (field.schema().getType() == Type.ARRAY) {
				this.arrayIndex = repeatIndex;
				this.arrayField = field;
				this.arrayItemSchema = field.schema().getElementType();
				this.arrayItemFields = arrayItemSchema.getFields();
				this.arrayCount = this.arrayItemFields.size();
			} else {
				this.fields.add(field);
			}
			repeatIndex++;
		}
		this.count = this.fields.size();
	}

	@Override
	public Record transform(byte[] bytes) throws TransformerException {
		String msg = new String(bytes);
		String[] items = msg.split("\\t");
		int repeatTimes = getRepeatTimes(items.length);
		if (repeatTimes == -1) {
			throw new RuntimeException(msg);
		}
		List<String[]> repeatedData = null;
		if (repeatTimes != 0) {
			repeatedData = new LinkedList<String[]>();
			int from = arrayIndex;
			for (int i = 0; i < repeatTimes; i++) {
				repeatedData.add(Arrays.copyOfRange(items, from, from
						+ arrayCount));
				from += arrayCount;
			}

			String[] newItems = new String[count];
			String[] headItems = null;
			if (arrayIndex > 0) {
				headItems = Arrays.copyOfRange(items, 0, arrayIndex);
				System.arraycopy(headItems, 0, newItems, 0, headItems.length);
			}
			if (count > arrayIndex) {
				String[] tailItems = Arrays.copyOfRange(items, arrayIndex
						+ repeatTimes * arrayCount, items.length);
				System.arraycopy(tailItems, 0, newItems, headItems == null ? 0
						: headItems.length, tailItems.length);
			}
			items = newItems;
		}
		Record record = newRecord();
		for (int i = 0; i < items.length; i++) {
			String item = items[i];
			record.put(i, getFieldValue(fields.get(i), item));
		}

		if (repeatedData != null) {
			List<Record> list = new ArrayList<Record>();
			for (String[] repeatedItem : repeatedData) {
				Record arrayItem = newArrayItemRecord();
				for (int i = 0; i < repeatedItem.length; i++) {
					String item = repeatedItem[i];
					arrayItem.put(i,
							getFieldValue(arrayItemFields.get(i), item));
				}
				list.add(arrayItem);
			}
			record.put(arrayIndex, list);
		}

		return record;
	}

	private Object getFieldValue(Field field, String item)
			throws TransformerException {
		Type type = null;
		if (fieldTypeCache.containsKey(field)) {
			type = fieldTypeCache.get(field);
		} else {
			type = getFieldType(field.schema());
			fieldTypeCache.put(field, type);
		}
		return AvroUtils.get(type, item);
	}

	public static Type getFieldType(Schema schema) {
		Type type = null;
		if (schema.getType() == Type.UNION) {
			List<Schema> schemas = schema.getTypes();
			for (Schema itemSchema : schemas) {
				Type itemType = getFieldType(itemSchema);
				if (itemType == Type.NULL) {
					if (type == null) {
						type = Type.NULL;
					}
				} else {
					type = itemType;
				}
			}
		} else {
			type = schema.getType();
		}
		return type;
	}

	public static Record newRecord(Schema schema) {
		return new GenericData.Record(schema);
	}

	public Record newRecord() {
		return new GenericData.Record(schema);
	}

	public Record newArrayItemRecord() {
		return new GenericData.Record(arrayItemSchema);
	}

	public int getRepeatTimes(int length) {
		if (arrayField == null) {
			if (length == this.count) {
				return 0;
			}
		} else {
			int repeatedTotal = length - this.count;
			if (repeatedTotal % arrayCount == 0) {
				return repeatedTotal / arrayCount;
			}
		}
		return -1;
	}

	public static Schema getKafkaTopicSchema(String topic) throws IOException {
		InputStream inputStream = KafkaTerminalAvroTransformer.class
				.getClassLoader()
				.getResourceAsStream("avro/" + topic + ".avro");
		return new Parser().parse(inputStream);
	}

	public static void main(String[] args) throws TransformerException,
			IOException {
		String str = "13925774514353362612	1401781295	365829	1401874072	40	43636	1	873897440	810488	0	10	703741367	1	1044	68605	32842	173	125377	228250	43016	16	737295799	1	1040	65050	0	0	107562	200351	32420	14	4205515121	1	971	63480	12212	69	93053	176642	38418	8	770850231	1	1046	69887	16421	96	77893	169529	56359	13	1515138363	1	167	129469	82105	1199	64921	93694	93532	25	703741367	1	0	0	0	0	0	0	0	34	737295799	1	0	0	0	0	0	0	0	38	770850231	1	0	0	0	0	0	0	0	46	4205515121	1	0	0	0	0	0	0	0	48	1515138363	1	0	0	0	0	0	0	0	49	1617807";
		KafkaTerminalAvroTransformer transformer = new KafkaTerminalAvroTransformer(
				getKafkaTopicSchema("t_playalive_v3"));
		Record r = transformer.transform(str.getBytes());
		System.out.println(r);
	}

}
