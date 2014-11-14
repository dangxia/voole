package com.voole.dungbeetle.order.record;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ApplicationContextUtil {
	public volatile static ClassPathXmlApplicationContext _cxt;
	public static final AtomicLong _size = new AtomicLong(0);

	public static ClassPathXmlApplicationContext getCxt(
			boolean isAutoRefreshCache) {
		if (_cxt == null) {
			createCxt(isAutoRefreshCache);
		}
		return _cxt;
	}

	public synchronized static ClassPathXmlApplicationContext createCxt(
			boolean isAutoRefreshCache) {
		_size.incrementAndGet();
		if (_cxt == null) {
			ClassPathXmlApplicationContext cxt = null;
			if (isAutoRefreshCache) {
				cxt = new ClassPathXmlApplicationContext(
						"classpath:/spring/spring-order-detail-cache-auto-refresh.xml");
			} else {
				cxt = new ClassPathXmlApplicationContext(
						"classpath:/spring/spring-order-detail-cache.xml");
			}
			_cxt = cxt;
		}
		return _cxt;

	}

	public synchronized static void closeCxt() {
		if (_size.decrementAndGet() == 0l) {
			_cxt.close();
			_cxt = null;
		}
	}
}
