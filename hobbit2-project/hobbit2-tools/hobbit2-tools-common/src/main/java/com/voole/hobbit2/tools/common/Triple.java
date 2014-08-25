/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common;

import java.io.Serializable;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public class Triple<A, B, C> implements Serializable {
	private A a;
	private B b;
	private C c;

	public Triple(A a, B b, C c) {
		this.a = a;
		this.b = b;
		this.c = c;
	}

	public A getA() {
		return a;
	}

	public void setA(A a) {
		this.a = a;
	}

	public B getB() {
		return b;
	}

	public void setB(B b) {
		this.b = b;
	}

	public C getC() {
		return c;
	}

	public void setC(C c) {
		this.c = c;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(a, b, c);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (obj instanceof Triple) {
			Triple<?, ?, ?> that = (Triple<?, ?, ?>) obj;
			return Objects.equal(this.a, that.a)
					&& Objects.equal(this.b, that.b)
					&& Objects.equal(this.c, that.c);
		}
		return false;
	}

}
