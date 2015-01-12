package com.voole.hobbit2.storm.test.kafka;

import storm.trident.spout.ISpoutPartition;

public class KafkaSpoutPartition implements ISpoutPartition {

	private final long longId;

	private final String id;

	public KafkaSpoutPartition(String name, Long id) {
		this.longId = id;
		this.id = name + "_" + id;
	}

	@Override
	public String getId() {
		return id;
	}

	public long getLongId() {
		return longId;
	}

}