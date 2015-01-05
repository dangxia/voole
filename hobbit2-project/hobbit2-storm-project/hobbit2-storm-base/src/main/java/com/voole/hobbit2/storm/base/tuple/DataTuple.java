package com.voole.hobbit2.storm.base.tuple;

import com.esotericsoftware.kryo.Kryo;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class DataTuple {

	private Fields outFields;
	private Fields inputFields;
	private Kryo kryo;

	public abstract Values getValues();

	public abstract void init(Tuple tuple);

	public Fields getOutFields() {
		return outFields;
	}

	public void setOutFields(Fields outFields) {
		this.outFields = outFields;
	}

	public Fields getInputFields() {
		return inputFields;
	}

	public void setInputFields(Fields inputFields) {
		this.inputFields = inputFields;
	}

}
