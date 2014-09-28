package com.voole.hobbit2.storm.order;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.google.common.base.Joiner;
import com.voole.hobbit2.camus.order.dry.PlayBgnDryRecord;

public class HiveHbaseTest {
	public static void main(String[] args) {
		Schema schema = PlayBgnDryRecord.getClassSchema();
		List<Field> fields = schema.getFields();
		List<String> colums = new ArrayList<String>();
		List<String> cfs = new ArrayList<String>();
		colums.add("key string");
		cfs.add(":key");
		for (Field field : fields) {
			cfs.add("cf:" + field.name());
			colums.add(field.name() + " " + getColumnType(field.schema()));
		}
		colums.add("playAliveTime bigint");
		colums.add("playEndTime bigint");

		cfs.add("cf:playAliveTime");
		cfs.add("cf:playEndTime");

		String createSql = "CREATE EXTERNAL TABLE storm_order_session("
				+ Joiner.on(", ").join(colums) + ") \r\n";
		createSql += "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\r\n";
		createSql += "WITH SERDEPROPERTIES ('hbase.columns.mapping' = '"
				+ Joiner.on(", ").join(cfs) + "')\r\n";
		createSql += "TBLPROPERTIES('hbase.table.name' = 'storm_order_session','hbase.table.default.storage.type' = 'binary');\r\n";

		System.out.println(createSql);
	}

	public static String getColumnType(Schema schema) {
		Type type = getType(schema);
		if (type == Type.LONG) {
			return "bigint";
		}
		return type.toString().toLowerCase();
	}

	public static Type getType(Schema schema) {
		if (schema.getType() == Type.UNION) {
			for (Schema subSchema : schema.getTypes()) {
				Type type = getType(subSchema);
				if (type != Type.NULL) {
					return type;
				}
			}
		} else {
			return schema.getType();
		}
		return Type.NULL;
	}
}
