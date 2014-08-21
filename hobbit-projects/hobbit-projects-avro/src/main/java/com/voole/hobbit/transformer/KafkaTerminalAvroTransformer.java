/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit.util.AvroUtils;

/**
 * @author XuehuiHe
 * @date 2014年7月10日
 */
public class KafkaTerminalAvroTransformer implements KafkaTransformer {

	private Schema schema;

	private List<Field> fields;
	private int count;

	private Field arrayField;
	private int arrayCount;
	private int arrayIndex;

	private List<Field> arrayItemFields;
	private Schema arrayItemSchema;
	private Map<Field, Type> fieldTypeCache;

	private Class<? extends SpecificRecordBase> mainClass;
	private Class<? extends SpecificRecordBase> arrayClass;

	public KafkaTerminalAvroTransformer(Schema schema)
			throws KafkaTransformException {
		try {
			this.fieldTypeCache = new HashMap<Schema.Field, Schema.Type>();
			this.schema = schema;
			this.mainClass = AvroUtils.getSchemaClass(schema);
			List<Field> fields = schema.getFields();
			this.fields = new ArrayList<Field>();
			int repeatIndex = 0;
			for (Field field : fields) {
				Schema arraySchema = getArraySchema(field.schema());
				if (arraySchema != null) {
					this.arrayIndex = repeatIndex;
					this.arrayField = field;
					this.arrayItemSchema = arraySchema.getElementType();
					this.arrayItemFields = arrayItemSchema.getFields();
					this.arrayCount = this.arrayItemFields.size();
					this.arrayClass = AvroUtils
							.getSchemaClass(this.arrayItemSchema);
				} else {
					this.fields.add(field);
				}
				repeatIndex++;
			}
			this.count = this.fields.size();
		} catch (Exception e) {
			throw new KafkaTransformException("Schema:" + schema.getFullName()
					+ " init failed", e);
		}

	}

	@Override
	public SpecificRecordBase transform(byte[] bytes)
			throws TransformerException {
		String msg = new String(bytes);
		String[] items = msg.split("\\t");
		int repeatTimes = getRepeatTimes(items.length);
		if (repeatTimes == -1) {
			throw new TransformerException("Schema:" + schema.getFullName()
					+ "\tMsg:" + msg + ",repeatTimes is wrong");
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
		SpecificRecordBase record = newMainRecord();
		for (int i = 0; i < items.length; i++) {
			String item = items[i];
			Field f = fields.get(i);
			record.put(f.pos(), getFieldValue(f, item));
		}

		if (repeatedData != null) {
			List<SpecificRecordBase> list = new ArrayList<SpecificRecordBase>();
			for (String[] repeatedItem : repeatedData) {
				SpecificRecordBase arrayItem = newArrayItemRecord();
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

	public static Schema getArraySchema(Schema schema) {
		if (schema.getType() == Type.UNION) {
			List<Schema> schemas = schema.getTypes();
			for (Schema itemSchema : schemas) {
				if (itemSchema.getType() == Type.ARRAY) {
					return itemSchema;
				}
			}
		} else if (schema.getType() == Type.ARRAY) {
			return schema;
		}
		return null;
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

	public Schema getSchema() {
		return schema;
	}

	public SpecificRecordBase newMainRecord() {
		try {
			return this.mainClass.newInstance();
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		}
		return null;
	}

	public SpecificRecordBase newArrayItemRecord() {
		try {
			return this.arrayClass.newInstance();
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		}
		return null;
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

	public static void main(String[] args) throws TransformerException,
			IOException {
		String str = "13925774514353362612	1401781295	365829	1401874072	40	43636	1	873897440	810488	0	10	703741367	1	1044	68605	32842	173	125377	228250	43016	16	737295799	1	1040	65050	0	0	107562	200351	32420	14	4205515121	1	971	63480	12212	69	93053	176642	38418	8	770850231	1	1046	69887	16421	96	77893	169529	56359	13	1515138363	1	167	129469	82105	1199	64921	93694	93532	25	703741367	1	0	0	0	0	0	0	0	34	737295799	1	0	0	0	0	0	0	0	38	770850231	1	0	0	0	0	0	0	0	46	4205515121	1	0	0	0	0	0	0	0	48	1515138363	1	0	0	0	0	0	0	0	49	1617807";
		str = "145	0	30	1305905550	BC83A71935BE00000000000000000000	1000	1694542016	8546749285776193522	0	0	FF2D905B74EE3A7A5C8CBC352BC3FC20	1363914803	7	992688	220	vosp://cdn.voole.com:3528/play?fid=ff2d905b74ee3a7a5c8cbc352bc3fc20&keyid=0&stamp=1406184247&is3d=0&fm=7&tvid=BC83A71935BE&bit=1300&auth=6caba6f89099539475e09a81e9696066&ext=oid:433,eid:100105,code:ASTBOX_movie_index&s=1	1406184279	0	2874777552";

		KafkaTransformer transformer = KafkaTransformerFactory
				.getTransformer("t_playbgn_v2");
		SpecificRecordBase r = transformer.transform(str.getBytes());
		System.out.println(r.getClass());
		System.out.println(r);
	}

}
