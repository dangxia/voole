/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api.model;

import org.apache.hive.service.cli.Type;

import com.google.common.base.Preconditions;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class HiveTablePartition {
	private final String name;
	private final Type type;

	public HiveTablePartition(String name, Type type) {
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
