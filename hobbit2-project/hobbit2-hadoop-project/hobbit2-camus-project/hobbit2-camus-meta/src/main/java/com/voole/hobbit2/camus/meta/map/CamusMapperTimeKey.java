package com.voole.hobbit2.camus.meta.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;

public class CamusMapperTimeKey implements CamusMapperKey<CamusMapperTimeKey> {
	private String topic;
	private long categoryTime;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getCategoryTime() {
		return categoryTime;
	}

	public void setCategoryTime(long categoryTime) {
		this.categoryTime = categoryTime;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, topic);
		WritableUtils.writeVLong(out, this.categoryTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.topic = WritableUtils.readString(in);
		this.categoryTime = WritableUtils.readVLong(in);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(topic, categoryTime);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof CamusMapperTimeKey) {
			CamusMapperTimeKey that = (CamusMapperTimeKey) obj;
			return this.compareTo(that) == 0;
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("topic", topic)
				.add("categoryTime", categoryTime).toString();
	}

	@Override
	public int compareTo(CamusMapperTimeKey o) {
		int c = this.topic.compareTo(o.topic);
		if (c == 0) {
			long diff = this.categoryTime - o.categoryTime;
			if (diff > 0) {
				return 1;
			} else if (diff < 0) {
				return -1;
			} else {
				return 0;
			}
		}
		return c;
	}

}
