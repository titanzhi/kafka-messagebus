package com.s3d.messagebus.kafka.loader;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.s3d.messagebus.kafka.context.MessagebusContextLoader;

/**
 * 在web.xml加入listener,负责 MessagebusContext 初始化
 * @author sulta
 */
public class MessagebusContextListener implements ServletContextListener {
	
	private MessagebusContextLoader contextLoader;

	public void contextInitialized(ServletContextEvent event) {
		this.contextLoader = createContextLoader();
		this.contextLoader.initContext(event.getServletContext());
	}

	protected MessagebusContextLoader createContextLoader() {
		return new MessagebusContextLoader();
	}

	public MessagebusContextLoader getContextLoader() {
		return contextLoader;
	}

	public void contextDestroyed(ServletContextEvent event) {
		if (this.contextLoader != null) {
			this.contextLoader.closeContext(event.getServletContext());
		}
	}
}
