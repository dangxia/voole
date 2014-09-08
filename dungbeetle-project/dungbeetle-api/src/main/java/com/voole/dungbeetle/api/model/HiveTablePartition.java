package com.voole.dungbeetle.api.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hive.service.cli.Type;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class HiveTablePartition implements Writable {
	private String name;
	private Type type;
	private Object value;

	public HiveTablePartition() {
	}

	protected HiveTablePartition(String name, Type type) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(type);
		this.name = name;
		this.type = type;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public Type getType() {
		return type;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(name, type, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && this.getClass() == obj.getClass()) {
			HiveTablePartition that = (HiveTablePartition) obj;
			return Objects.equal(this.name, that.name)
					&& Objects.equal(this.type, that.type)
					&& Objects.equal(this.value, that.value);
		}
		return false;
	}

	protected void writeValue(DataOutput out) throws IOException {
		switch (type) {
		case STRING_TYPE:
			WritableUtils.writeString(out, (String) value);
			break;
		case CHAR_TYPE:
			out.writeChar((Character) value);
			break;
		case INT_TYPE:
			WritableUtils.writeVInt(out, (Integer) value);
			break;
		case BIGINT_TYPE:
			WritableUtils.writeVLong(out, (Long) value);
			break;
		case FLOAT_TYPE:
			out.writeFloat((Float) value);
			break;
		case DOUBLE_TYPE:
			out.writeDouble((Double) value);
			break;
		case BOOLEAN_TYPE:
			out.writeBoolean((Boolean) value);
			break;
		default:
			break;
		}
	}

	protected Object readValue(DataInput in) throws IOException {
		switch (type) {
		case STRING_TYPE:
			return WritableUtils.readString(in);
		case CHAR_TYPE:
			return in.readChar();
		case INT_TYPE:
			return WritableUtils.readVInt(in);
		case BIGINT_TYPE:
			return WritableUtils.readVLong(in);
		case FLOAT_TYPE:
			return in.readFloat();
		case DOUBLE_TYPE:
			return in.readDouble();
		case BOOLEAN_TYPE:
			return in.readBoolean();
		default:
			break;
		}
		return null;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, name);
		WritableUtils.writeEnum(out, getType());
		writeValue(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = WritableUtils.readString(in);
		this.type = WritableUtils.readEnum(in, Type.class);
		this.value = readValue(in);
	}

}
