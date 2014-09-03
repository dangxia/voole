/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class TestCamusKey implements ICamusKey, Cloneable {
	@Override
	public TestCamusKey clone() throws CloneNotSupportedException {
		return new TestCamusKey(this.name);
	}

	private String name;

	public TestCamusKey(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof TestCamusKey) {
			TestCamusKey that = (TestCamusKey) obj;
			return Objects.equal(this.name, that.name);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("name", name).toString();
	}

}
