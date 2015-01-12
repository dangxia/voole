package com.voole.hobbit2.storm.order2.partition;

import com.google.common.base.Objects;

public class HdfsNoendFilePartition extends HdfsKafkaMixedSpoutPartition
		implements Comparable<HdfsNoendFilePartition> {
	private String fileName;
	private String topic;

	public HdfsNoendFilePartition() {
		super(Type.HDFS);
	}

	public HdfsNoendFilePartition(String fileName, String topic) {
		super(Type.HDFS);
		this.fileName = fileName;
		this.topic = topic;
	}

	@Override
	public String getId() {
		return fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public String getTopic() {
		return topic;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public int hashCode() {
		return getFileName().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof HdfsNoendFilePartition) {
			HdfsNoendFilePartition other = (HdfsNoendFilePartition) obj;
			return this.getFileName().equals(other.getFileName());
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("fileName", getFileName())
				.add("topic", getTopic()).toString();
	}

	@Override
	public int compareTo(HdfsNoendFilePartition o) {
		return this.getFileName().compareTo(o.getFileName());
	}
}
