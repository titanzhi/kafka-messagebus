package com.s3d.messagebus.kafka.context;

import javax.servlet.ServletContext;

import com.s3d.messagebus.kafka.IKafkaConsumerManager;


public interface IMessagebusContext {

	String CONTEXT_ATTRIBUTE = IMessagebusContext.class.getName() + ".ROOT";

	/**
	 * 获得Servlet上下文
	 */
	ServletContext getServletContext();
		
	/**
	 * 获取KafkaConsumerManager
	 * @return
	 */
	IKafkaConsumerManager getKafkaConsumerManager();
	
	/**
	 * 关闭
	 */
	void close();
}
