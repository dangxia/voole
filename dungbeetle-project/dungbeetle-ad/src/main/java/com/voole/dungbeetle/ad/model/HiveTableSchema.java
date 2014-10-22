/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.ad.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hive.service.cli.Type;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class HiveTableSchema {
	final private String tableName;
	final private List<HiveTablePartitionSchema> partitions;
	final private Schema schema;

	public HiveTableSchema(String tableName, List<HiveTablePartitionSchema> partitions,
			Schema schema) {
		this.tableName = tableName;
		this.partitions = partitions;
		this.schema = schema;
	}

	public HiveTableSchema(String tableName, Schema schema) {
		this(tableName, null, schema);
	}

	public boolean hasPartitions() {
		return partitions != null && partitions.size() > 0;
	}

	public String getTableName() {
		return tableName;
	}

	public List<HiveTablePartitionSchema> getPartitions() {
		return partitions;
	}

	public Schema getSchema() {
		return schema;
	}

	public static String getPartitionSchema(HiveTableSchema table) {
		List<String> partitionStrList = new ArrayList<String>();
		for (HiveTablePartitionSchema partition : table.getPartitions()) {
			partitionStrList.add(partition.getName() + " "
					+ partition.getType().getName());
		}
		return Joiner.on(" , ").join(partitionStrList);
	}

	public static String getCreateHiveTableSchema(HiveTableSchema table) {
		StringBuffer sb = new StringBuffer("CREATE TABLE ");
		sb.append(table.getTableName() + "\n ");
		if (table.hasPartitions()) {
			sb.append(" PARTITIONED BY( " + getPartitionSchema(table) + " )\n ");
		}
		sb.append(" ROW FORMAT SERDE \n  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \n  STORED AS INPUTFORMAT \n  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n  OUTPUTFORMAT \n  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n");
		sb.append(" TBLPROPERTIES (\n 'avro.schema.literal'=\n'");
		sb.append(table.getSchema().toString());
		sb.append("'\n)");
		return sb.toString();
	}
	
	public static class HiveTablePartitionSchema {
		private final String name;
		private final Type type;

		public HiveTablePartitionSchema(String name, Type type) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(type);
			this.name = name;
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public Type getType() {
			return type;
		}

	}
}
