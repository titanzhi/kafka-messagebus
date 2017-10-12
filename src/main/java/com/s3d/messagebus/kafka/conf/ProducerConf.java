package com.s3d.messagebus.kafka.conf;

/**
 * 生产者配置
 * @author sulta
 *
 */
public class ProducerConf {

	/**
	 * broker地址列表
	 */
	private String metaBrokerList;
	/**
	 * serializer.class
	 */
	private String serializerClass;
	/**
	 * request.required.acks
	 */
	private String requestRequiredAcks;

	public String getMetaBrokerList() {
		return metaBrokerList;
	}

	public String getSerializerClass() {
		return serializerClass;
	}

	public String getRequestRequiredAcks() {
		return requestRequiredAcks;
	}

	protected void setMetaBrokerList(String metaBrokerList) {
		this.metaBrokerList = metaBrokerList;
	}
	
	protected void setSerializerClass(String serializerClass) {
		this.serializerClass = serializerClass;
	}

	protected void setRequestRequiredAcks(String requestRequiredAcks) {
		this.requestRequiredAcks = requestRequiredAcks;
	}
	
}
