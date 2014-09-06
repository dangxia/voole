/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.google.common.base.Joiner;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class HiveTable {
	final private String tableName;
	final private List<HiveTablePartition> partitions;
	final private Schema schema;

	public HiveTable(String tableName, List<HiveTablePartition> partitions,
			Schema schema) {
		this.tableName = tableName;
		this.partitions = partitions;
		this.schema = schema;
	}

	public HiveTable(String tableName, Schema schema) {
		this(tableName, null, schema);
	}

	public boolean hasPartitions() {
		return partitions != null && partitions.size() > 0;
	}

	public String getTableName() {
		return tableName;
	}

	public List<HiveTablePartition> getPartitions() {
		return partitions;
	}

	public Schema getSchema() {
		return schema;
	}

	public static String getPartitionSchema(HiveTable table) {
		List<String> partitionStrList = new ArrayList<String>();
		for (HiveTablePartition partition : table.getPartitions()) {
			partitionStrList.add(partition.getName() + " "
					+ partition.getType().getName());
		}
		return Joiner.on(" , ").join(partitionStrList);
	}

	public static String getCreateHiveTableSchema(HiveTable table) {
		StringBuffer sb = new StringBuffer("CREATE TABLE ");
		sb.append(table.getTableName() + "\n ");
		if (table.hasPartitions()) {
			sb.append(" PARTITIONED BY( " + getPartitionSchema(table) + " )\n ");
		}
		sb.append(" ROW FORMAT SERDE \n  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \n  STORED AS INPUTFORMAT \n  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n  OUTPUTFORMAT \n  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n");
		sb.append(" TBLPROPERTIES (\n 'avro.schema.literal'='\n ");
		sb.append(table.getSchema().toString());
		sb.append("  ')");
		return sb.toString();
	}
}
