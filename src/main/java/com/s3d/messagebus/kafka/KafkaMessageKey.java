package com.s3d.messagebus.kafka;

/**
 * 
 * @author sulta
 *
 */
public class KafkaMessageKey {
	public KafkaMessageKey() {
	}
	/**
	 * 消息id
	 */
	private Long id;
	
	/**
	 * 消息所在topic
	 */
	private String topic;
	
	/**
	 * 创建的时间
	 */
	protected Long createTimeStamp; 
	
	/**
	 * 收到的时间
	 */
	protected Long receiveTimeStamp;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getCreateTimeStamp() {
		return createTimeStamp;
	}

	public void setCreateTimeStamp(Long createTimeStamp) {
		this.createTimeStamp = createTimeStamp;
	}

	public Long getReceiveTimeStamp() {
		return receiveTimeStamp;
	}

	public void setReceiveTimeStamp(Long receiveTimeStamp) {
		this.receiveTimeStamp = receiveTimeStamp;
	}
}
