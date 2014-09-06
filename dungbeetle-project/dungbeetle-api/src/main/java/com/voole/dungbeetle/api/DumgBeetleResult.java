/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;

import com.google.common.base.Preconditions;
import com.voole.dungbeetle.api.model.HiveTable;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class DumgBeetleResult {
	private final HiveTable table;
	private final Map<String, Object> partitionValueMap;
	private final List<SpecificRecordBase> records;

	public DumgBeetleResult(HiveTable table) {
		this(table, new HashMap<String, Object>());
	}

	public DumgBeetleResult(HiveTable table,
			Map<String, Object> partitionValueMap) {
		this.table = table;
		this.partitionValueMap = partitionValueMap;
		this.records = new ArrayList<SpecificRecordBase>();
	}

	public DumgBeetleResult(HiveTable table,
			Map<String, Object> partitionValueMap,
			List<SpecificRecordBase> records) {
		this.table = table;
		this.partitionValueMap = partitionValueMap;
		this.records = records;
	}

	protected HiveTable getTable() {
		return table;
	}

	protected Map<String, Object> getPartitionValueMap() {
		return partitionValueMap;
	}

	protected List<SpecificRecordBase> getRecords() {
		return records;
	}

	public void partition(String name, Object value) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(value);
		this.partitionValueMap.put(name, value);
	}

	public void addRecord(SpecificRecordBase record) {
		getRecords().add(record);
	}

	public void addAllRecords(Collection<? extends SpecificRecordBase> records) {
		getRecords().addAll(records);
	}

	public void clearPartitions() {
		getPartitionValueMap().clear();
	}

	public void clearRecords() {
		getRecords().clear();
	}

}
