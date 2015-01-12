package com.voole.hobbit2.storm.test.stream;

import storm.trident.spout.ISpoutPartition;

public class TestStreamSpoutPartition implements ISpoutPartition {

	private final long longId;

	private final String id;

	public TestStreamSpoutPartition(String name, Long id) {
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