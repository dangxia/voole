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
import com.voole.hobbit2.common.Tuple;

public class PhoenixUtils {
	public static String psName = "ps";

	public static void main(String[] args) {
		Set<String> allExcludeColumns = new HashSet<String>();
		allExcludeColumns.add("sessid");

		Schema schema = HiveOrderDetailRecord.getClassSchema();
		System.out.println(getCreateSinglePkPhoenixTableSql(schema, "sessid",
				String.class, allExcludeColumns));
		List<Tuple<String, Class<?>>> keyInfos = new ArrayList<Tuple<String, Class<?>>>();
		// keyInfos.add(new Tuple<String, Class<?>>("hour", String.class));
		keyInfos.add(new Tuple<String, Class<?>>("day", String.class));
		keyInfos.add(new Tuple<String, Class<?>>("dim_oem_id", Long.class));
		keyInfos.add(new Tuple<String, Class<?>>("sessid", String.class));
		System.out.println(getCreateMultiPkPhoenixTableSql(schema,
				"fact_vod_history", keyInfos, null));

		psName = "bgnPs";
		Set<String> bgnExcludeColumns = new HashSet<String>();
		bgnExcludeColumns.add("metric_playalivetime");
		bgnExcludeColumns.add("metric_playendtime");
		bgnExcludeColumns.add("metric_avgspeed");
		bgnExcludeColumns.add("sessid");
		getUpdateInsertSql(schema, "sessid", String.class, null,
				bgnExcludeColumns);

		System.out.println("------------alive-------");
		psName = "alivePs";
		Set<String> aliveIncludeColumns = new HashSet<String>();
		aliveIncludeColumns.add("metric_playalivetime");
		aliveIncludeColumns.add("metric_avgspeed");
		getUpdateInsertSql(schema, "HiveOrderDetailRecord_phoenix", "sessid",
				String.class, aliveIncludeColumns, null);
		System.out.println("------------end-------");
		psName = "endPs";
		Set<String> endIncludeColumns = new HashSet<String>();
		endIncludeColumns.add("metric_playendtime");
		endIncludeColumns.add("metric_avgspeed");
		getUpdateInsertSql(schema, "HiveOrderDetailRecord_phoenix", "sessid",
				String.class, endIncludeColumns, null);

		moveDate(schema, 2);
	}

	public static void moveDate(Schema schema, int start) {
		List<Field> fields = schema.getFields();
		int index = start;
		for (Field field : fields) {
			Class<?> fieldType = avroTypeToJavaClass.get(getFieldType(field
					.schema()));
			String name = field.name();
			System.out.println("// " + name);
			if (fieldType == String.class) {
				System.out.println("ps.setString(" + index + ", qs.getString("
						+ index + "));");
			} else {
				System.out.println(fieldType.getSimpleName() + " tmp" + index
						+ "=qs." + javaClassToPsGetMethod.get(fieldType) + "("
						+ index + ");");
				System.out.println("if (qs.wasNull()) {");
				System.out.println("ps.setNull(" + index + ", "
						+ javaClassToSqlType.get(fieldType) + ");");
				System.out.println(" } else {");
				System.out.println("ps."
						+ javaClassToPsSetMethod.get(fieldType) + "(" + index
						+ ", tmp" + index + ");");
				System.out.println("}");
			}
			index++;
		}
	}

	public static void getUpdateInsertSql(Schema schema, String keyName,
			Class<?> keyType, Set<String> includeColumns,
			Set<String> excludeColumns) {
		getUpdateInsertSql(schema, schema.getName() + "_phoenix", keyName,
				keyType, includeColumns, excludeColumns);
	}

	public static void getUpdateInsertSql(Schema schema, String tableName,
			String keyName, Class<?> keyType, Set<String> includeColumns,
			Set<String> excludeColumns) {
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
			if ((includeColumns == null || includeColumns.size() == 0 || includeColumns
					.contains(name))
					&& (excludeColumns == null || !excludeColumns
							.contains(name))) {
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
			String name = columns.get(i - 1);
			psColumn(i, type, name);
		}
	}

	public static void psColumn(int i, Class<?> type, String name) {
		if (type == String.class) {
			System.out.println(psName + "." + javaClassToPsSetMethod.get(type)
					+ "(" + i + ", String.valueOf(record."
					+ instanceGetMethodName(name) + "()));");
		} else {
			String getMethodName = instanceGetMethodName(name);
			System.out.println("if(record." + getMethodName + "()==null){");
			System.out.println(psName + ".setNull(" + i + ", "
					+ javaClassToSqlType.get(type) + ");");
			System.out.println("}else{");
			System.out.println(psName + "." + javaClassToPsSetMethod.get(type)
					+ "(" + i + ", record." + getMethodName + "());");
			System.out.println("}");
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

	public static String getCreateMultiPkPhoenixTableSql(Schema schema,
			String tableName, List<Tuple<String, Class<?>>> keyInfos,
			Set<String> excludeColumns) {
		List<Field> fields = schema.getFields();
		List<String> columnSqls = new ArrayList<String>();
		List<String> writedColumns = new ArrayList<String>();

		List<String> pkColumns = new ArrayList<String>();
		for (Tuple<String, Class<?>> tuple : keyInfos) {
			pkColumns.add(tuple.getA());
			writedColumns.add(tuple.getA());
			columnSqls.add(tuple.getA() + " "
					+ javaClassToPhoenixType.get(tuple.getB()));
		}

		for (Field field : fields) {
			if (writedColumns.contains(field.name())) {
				continue;
			}
			if (excludeColumns == null || excludeColumns.size() == 0
					|| !excludeColumns.contains(field.name())) {
				writedColumns.add(field.name());
				columnSqls.add(field.name()
						+ " "
						+ javaClassToPhoenixType.get(avroTypeToJavaClass
								.get(getFieldType(field.schema()))));
			}
		}
		String columnSql = Joiner.on(',').join(columnSqls);
		String pkSql = " CONSTRAINT pk PRIMARY KEY ("
				+ Joiner.on(',').join(pkColumns) + ") ";
		String createSql = "CREATE TABLE " + tableName + " ( " + columnSql
				+ pkSql + ") SALT_BUCKETS=14, KEEP_DELETED_CELLS = false";

		System.out.println(Joiner.on(',').join(writedColumns));

		return createSql;
	}

	public static String getCreateSinglePkPhoenixTableSql(Schema schema,
			String keyName, Class<?> keyType, Set<String> excludeColumns) {
		return getCreateSinglePhoenixTableSql(schema, schema.getName()
				+ "_phoenix", keyName, keyType, excludeColumns);
	}

	public static String getCreateSinglePhoenixTableSql(Schema schema,
			String tableName, String keyName, Class<?> keyType,
			Set<String> excludeColumns) {
		List<Field> fields = schema.getFields();
		List<String> columnSqls = new ArrayList<String>();
		columnSqls.add(keyName + " " + javaClassToPhoenixType.get(keyType)
				+ " not null primary key");
		for (Field field : fields) {
			if (excludeColumns == null || excludeColumns.size() == 0
					|| !excludeColumns.contains(field.name())) {
				columnSqls.add(field.name()
						+ " "
						+ javaClassToPhoenixType.get(avroTypeToJavaClass
								.get(getFieldType(field.schema()))));
			}
		}
		String columnSql = Joiner.on(',').join(columnSqls);
		String createSql = "CREATE TABLE "
				+ tableName
				+ " ( "
				+ columnSql
				+ ") SALT_BUCKETS=14 , KEEP_DELETED_CELLS = false, IN_MEMORY = true, TTL = 18000";
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

	public static Map<Class<?>, String> javaClassToPsGetMethod = new HashMap<Class<?>, String>();

	static {
		javaClassToPsGetMethod.put(Integer.class, "getInt");
		javaClassToPsGetMethod.put(Long.class, "getLong");

		javaClassToPsGetMethod.put(Double.class, "getDouble");
		javaClassToPsGetMethod.put(Float.class, "getFloat");

		javaClassToPsGetMethod.put(Boolean.class, "getBoolean");

		javaClassToPsGetMethod.put(String.class, "getString");

		javaClassToPsGetMethod = ImmutableMap.copyOf(javaClassToPsGetMethod);
	}

	public static Map<Class<?>, String> javaClassToSqlType = new HashMap<Class<?>, String>();

	static {
		javaClassToSqlType.put(Integer.class, "Types.INTEGER");
		javaClassToSqlType.put(Long.class, "Types.BIGINT");

		javaClassToSqlType.put(Double.class, "Types.DOUBLE");
		javaClassToSqlType.put(Float.class, "Types.FLOAT");

		javaClassToSqlType.put(Boolean.class, "Types.BOOLEAN");

		javaClassToSqlType = ImmutableMap.copyOf(javaClassToSqlType);
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
