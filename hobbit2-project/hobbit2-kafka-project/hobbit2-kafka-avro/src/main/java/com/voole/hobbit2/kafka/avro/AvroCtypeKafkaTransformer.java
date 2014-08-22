/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;
import com.voole.hobbit2.tools.avro.AvroConverts;
import com.voole.hobbit2.tools.avro.AvroSchemas;

/**
 * @author XuehuiHe
 * @date 2014年7月10日
 */
public class AvroCtypeKafkaTransformer implements AvroKafkaTransformer {

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

	public AvroCtypeKafkaTransformer(Schema schema)
			throws KafkaTransformException {
		try {
			this.fieldTypeCache = new HashMap<Schema.Field, Schema.Type>();
			this.schema = schema;
			this.mainClass = AvroSchemas.getSchemaClass(schema);
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
					this.arrayClass = AvroSchemas
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
			throws KafkaTransformException {
		String msg = new String(bytes);
		String[] items = msg.split("\\t");
		int repeatTimes = getRepeatTimes(items.length);
		if (repeatTimes == -1) {
			throw new KafkaTransformException("Schema:" + schema.getFullName()
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
			throws KafkaTransformException {
		Type type = null;
		if (fieldTypeCache.containsKey(field)) {
			type = fieldTypeCache.get(field);
		} else {
			type = getFieldType(field.schema());
			fieldTypeCache.put(field, type);
		}
		try {
			return AvroConverts.convert(type, item);
		} catch (Exception e) {
			throw new KafkaTransformException(e);
		}

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

}
