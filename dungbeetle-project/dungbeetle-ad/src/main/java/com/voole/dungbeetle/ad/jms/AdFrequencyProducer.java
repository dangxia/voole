package com.voole.dungbeetle.ad.jms;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;


public class AdFrequencyProducer {
	static Log log = LogFactory.getLog(AdFrequencyProducer.class);

	public AdFrequencyProducer() {
	}
   
	private JmsTemplate jmsTemplate;
	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}
	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}
	
	
	public void sendByString(final String content) {
		System.out.println("JMS:成功发送了一条文本消息."+content);
		jmsTemplate.send(new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(content);
			}
		});
	}
	
	public void send(Map mapMeg){
		jmsTemplate.convertAndSend(mapMeg);
	}

}
