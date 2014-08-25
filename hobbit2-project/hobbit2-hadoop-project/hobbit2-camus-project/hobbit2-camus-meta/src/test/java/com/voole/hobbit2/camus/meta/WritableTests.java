/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public abstract class WritableTests {
	private DataOutputStream output;
	private ByteArrayOutputStream _out;
	private DataInputStream input;

	@Before
	public void before() {
		_out = new ByteArrayOutputStream();
		output = new DataOutputStream(_out);
	}

	@After
	public void after() throws IOException {
		if (output != null) {
			output.close();
			output = null;
		}
		if (input != null) {
			input.close();
			input = null;
		}
	}

	public DataOutputStream getOutput() {
		return output;
	}

	public void setOutput(DataOutputStream output) {
		this.output = output;
	}

	public void setInput(DataInputStream input) {
		this.input = input;
	}

	public DataInputStream getInput() {
		if (input == null) {
			input = new DataInputStream(new ByteArrayInputStream(
					_out.toByteArray()));
		}
		return input;
	}
}
