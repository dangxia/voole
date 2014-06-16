/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.utils;

/**
 * @author XuehuiHe
 * @date 2013年10月25日
 */
public class Tuple<A, B> {
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
		int code = 0;
		if (this.a != null) {
			code += this.a.hashCode();
		}
		if (this.b != null) {
			code += this.b.hashCode() * 3;
		}
		return code;
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
			return equalsB(that) && equalsA(that);
		}
		return false;
	}

	private boolean equalsA(Tuple<?, ?> that) {
		if (this.a == null && that.a == null) {
			return true;
		}
		if (this.a != null) {
			return this.a.equals(that.a);
		}
		return that.a.equals(this.a);
	}

	private boolean equalsB(Tuple<?, ?> that) {
		if (this.b == null && that.b == null) {
			return true;
		}
		if (this.b != null) {
			return this.b.equals(that.b);
		}
		return that.b.equals(this.b);
	}
}
