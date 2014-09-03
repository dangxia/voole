/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.common;

import java.io.Serializable;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2013年10月25日
 */
public class Tuple<A, B> implements Serializable {
	private A a;
	private B b;

	public Tuple() {
	}

	public Tuple(A a, B b) {
		this.a = a;
		this.b = b;
	}

	public A getA() {
		return a;
	}

	public B getB() {
		return b;
	}

	public void setA(A a) {
		this.a = a;
	}

	public void setB(B b) {
		this.b = b;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(a, b);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (obj instanceof Tuple) {
			Tuple<?, ?> that = (Tuple<?, ?>) obj;
			return Objects.equal(this.a, that.a)
					&& Objects.equal(this.b, that.b);
		}
		return false;
	}

}
