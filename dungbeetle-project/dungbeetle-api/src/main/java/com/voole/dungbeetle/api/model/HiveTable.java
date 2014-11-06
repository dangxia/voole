package com.voole.dungbeetle.api.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

public class HiveTable implements Writable {
	private String name;
	private List<HiveTablePartition> partitions;
	private Schema schema;

	public HiveTable() {
		this.partitions = new ArrayList<HiveTablePartition>();
	}

	public boolean hasPartition() {
		return partitions != null && partitions.size() > 0;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<HiveTablePartition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<HiveTablePartition> partitions) {
		this.partitions = partitions;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(name, partitions);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && this.getClass() == obj.getClass()) {
			HiveTable that = (HiveTable) obj;
			return Objects.equal(this.name, that.name)
					&& Objects.equal(this.partitions, that.partitions);
		}
		return false;
	}

	public String getFileName() {
		List<String> list = new ArrayList<String>();
		list.add(getName());
		for (HiveTablePartition partition : partitions) {
			list.add(partition.getName());
			list.add(partition.getValue().toString());
		}
		return Joiner.on(',').join(list);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, this.name);
		WritableUtils.writeVInt(out, partitions.size());
		for (HiveTablePartition partition : partitions) {
			partition.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = WritableUtils.readString(in);
		int size = WritableUtils.readVInt(in);
		partitions.clear();
		for (int i = 0; i < size; i++) {
			HiveTablePartition partition = new HiveTablePartition();
			partition.readFields(in);
			partitions.add(partition);
		}

	}

}
