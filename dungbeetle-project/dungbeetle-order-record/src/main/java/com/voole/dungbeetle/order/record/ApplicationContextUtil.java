package com.voole.dungbeetle.order.record;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ApplicationContextUtil {
	public volatile static ClassPathXmlApplicationContext _cxt;

	public synchronized static ClassPathXmlApplicationContext getCxt(
			boolean isAutoRefreshCache) {
		if (_cxt == null) {
			if (isAutoRefreshCache) {
				_cxt = new ClassPathXmlApplicationContext(
						"classpath:/spring/spring-order-detail-cache-auto-refresh.xml");
			} else {
				_cxt = new ClassPathXmlApplicationContext(
						"classpath:/spring/spring-order-detail-cache.xml");
			}
		}
		return _cxt;
	}
}
