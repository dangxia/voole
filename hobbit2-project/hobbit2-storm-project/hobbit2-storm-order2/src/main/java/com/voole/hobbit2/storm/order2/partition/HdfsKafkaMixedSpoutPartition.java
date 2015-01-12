package com.voole.hobbit2.storm.order2.partition;

import java.io.Serializable;

import storm.trident.spout.ISpoutPartition;

public abstract class HdfsKafkaMixedSpoutPartition implements ISpoutPartition,
		Serializable {
	public static enum Type {
		HDFS, KAFKA;
	}

	private final Type type;

	public HdfsKafkaMixedSpoutPartition(Type type) {
		this.type = type;
	}

	public Type getType() {
		return type;
	}
}
