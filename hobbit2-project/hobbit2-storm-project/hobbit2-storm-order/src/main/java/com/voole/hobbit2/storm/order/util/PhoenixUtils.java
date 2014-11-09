package com.voole.hobbit2.storm.order.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;

public class PhoenixUtils {
	public static void main(String[] args) {
		Schema schema = HiveOrderDetailRecord.getClassSchema();
		System.out.println(getCreateSinglePkPhoenixTableSql(schema, "id",
				String.class));
		getUpdateInsertSql(schema, "id", String.class, null);

		System.out.println("------------alive-------");
		Set<String> aliveIncludeColumns = new HashSet<String>();
		aliveIncludeColumns.add("metric_playalivetime");
		aliveIncludeColumns.add("metric_avgspeed");
		getUpdateInsertSql(schema, "HiveOrderDetailRecord_phoenix", "id",
				String.class, aliveIncludeColumns);
		System.out.println("------------end-------");

		Set<String> endIncludeColumns = new HashSet<String>();
		endIncludeColumns.add("metric_playendtime");
		endIncludeColumns.add("metric_avgspeed");
		getUpdateInsertSql(schema, "HiveOrderDetailRecord_phoenix", "id",
				String.class, aliveIncludeColumns);
	}

	public static void getUpdateInsertSql(Schema schema, String keyName,
			Class<?> keyType, Set<String> includeColumns) {
		getUpdateInsertSql(schema, schema.getName() + "_phoenix", keyName,
				keyType, includeColumns);
	}

	public static void getUpdateInsertSql(Schema schema, String tableName,
			String keyName, Class<?> keyType, Set<String> includeColumns) {
		List<Field> fields = schema.getFields();
		List<String> columns = new ArrayList<String>();
		List<Class<?>> columnTypes = new ArrayList<Class<?>>();
		List<String> values = new ArrayList<String>();

		// PK
		columns.add(keyName);
		columnTypes.add(keyType);
		values.add("?");

		for (Field field : fields) {
			String name = field.name();
			if (includeColumns == null || includeColumns.size() == 0
					|| includeColumns.contains(name)) {
				columns.add(name);
				columnTypes.add(avroTypeToJavaClass.get(getFieldType(field
						.schema())));
				values.add("?");
			}
		}
		String updateSql = "UPSERT INTO " + tableName + "("
				+ Joiner.on(',').join(columns) + ") VALUES ("
				+ Joiner.on(',').join(values) + ") ";
		System.out.println(updateSql);

		for (int i = 1; i <= columns.size(); i++) {
			Class<?> type = columnTypes.get(i - 1);
			if (type == String.class) {
				System.out.println("ps." + javaClassToPsSetMethod.get(type)
						+ "(" + i + ", String.valueOf(record."
						+ instanceGetMethodName(columns.get(i - 1)) + "()));");
			} else {
				System.out.println("ps." + javaClassToPsSetMethod.get(type)
						+ "(" + i + ", record."
						+ instanceGetMethodName(columns.get(i - 1)) + "());");
			}
		}
	}

	public static String instanceGetMethodName(String fieldName) {
		String[] items = fieldName.split("_");
		StringBuffer sb = new StringBuffer();
		for (String item : items) {
			sb.append(item.substring(0, 1).toUpperCase() + item.substring(1));
		}
		return "get" + sb.toString();
	}

	public static String getCreateSinglePkPhoenixTableSql(Schema schema,
			String keyName, Class<?> keyType) {
		return getCreateSinglePhoenixTableSql(schema, schema.getName()
				+ "_phoenix", keyName, keyType);
	}

	public static String getCreateSinglePhoenixTableSql(Schema schema,
			String tableName, String keyName, Class<?> keyType) {
		List<Field> fields = schema.getFields();
		List<String> columnSqls = new ArrayList<String>();
		columnSqls.add(keyName + " " + javaClassToPhoenixType.get(keyType)
				+ " not null primary key");
		for (Field field : fields) {
			columnSqls.add(field.name()
					+ " "
					+ javaClassToPhoenixType.get(avroTypeToJavaClass
							.get(getFieldType(field.schema()))));
		}
		String columnSql = Joiner.on(',').join(columnSqls);
		String createSql = "CREATE TABLE " + tableName + " ( " + columnSql
				+ ")";
		return createSql;
	}

	public static Map<Type, Class<?>> avroTypeToJavaClass = new HashMap<Type, Class<?>>();
	static {
		avroTypeToJavaClass.put(Type.INT, Integer.class);
		avroTypeToJavaClass.put(Type.BOOLEAN, Boolean.class);
		avroTypeToJavaClass.put(Type.DOUBLE, Double.class);
		avroTypeToJavaClass.put(Type.ENUM, Enum.class);
		avroTypeToJavaClass.put(Type.FLOAT, Float.class);
		avroTypeToJavaClass.put(Type.LONG, Long.class);
		avroTypeToJavaClass.put(Type.STRING, String.class);
		avroTypeToJavaClass = ImmutableMap.copyOf(avroTypeToJavaClass);
	}

	public static Map<Class<?>, String> javaClassToPhoenixType = new HashMap<Class<?>, String>();

	static {
		javaClassToPhoenixType.put(Integer.class, "INTEGER");
		javaClassToPhoenixType.put(Long.class, "BIGINT");

		javaClassToPhoenixType.put(Double.class, "DOUBLE");
		javaClassToPhoenixType.put(Float.class, "FLOAT");

		javaClassToPhoenixType.put(Boolean.class, "BOOLEAN");

		javaClassToPhoenixType.put(String.class, "VARCHAR");

		javaClassToPhoenixType = ImmutableMap.copyOf(javaClassToPhoenixType);
	}

	public static Map<Class<?>, String> javaClassToPsSetMethod = new HashMap<Class<?>, String>();

	static {
		javaClassToPsSetMethod.put(Integer.class, "setInt");
		javaClassToPsSetMethod.put(Long.class, "setLong");

		javaClassToPsSetMethod.put(Double.class, "setDouble");
		javaClassToPsSetMethod.put(Float.class, "setFloat");

		javaClassToPsSetMethod.put(Boolean.class, "setBoolean");

		javaClassToPsSetMethod.put(String.class, "setString");

		javaClassToPsSetMethod = ImmutableMap.copyOf(javaClassToPsSetMethod);
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

}
