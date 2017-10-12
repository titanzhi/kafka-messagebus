package com.s3d.messagebus.kafka.context;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息总线上下文加载器
 * @author sulta
 *
 */
public class MessagebusContextLoader {
	
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	
	private MessagebusContext context;
	
	public MessagebusContext initContext(ServletContext servletContext) {
		MessagebusContext context = (MessagebusContext) servletContext
				.getAttribute(IMessagebusContext.CONTEXT_ATTRIBUTE);
		
		if (context == null) {
			try {
				context = createMessagebusContext(servletContext);
				servletContext.setAttribute(
						IMessagebusContext.CONTEXT_ATTRIBUTE, context);
			} catch (Throwable ex) {
				LOG.error("kafka消息总线初始化失败", ex);
			}
		}
		
		this.context = context;

		return context;
	}
	
	private MessagebusContext createMessagebusContext(ServletContext servletContext) throws Exception {
		MessagebusContext context = new MessagebusContext(servletContext);		
		return context;
	}

	public void closeContext(ServletContext servletContext) {
		if (this.context instanceof IMessagebusContext) {
			((IMessagebusContext) this.context).close();
		}
	}

}
