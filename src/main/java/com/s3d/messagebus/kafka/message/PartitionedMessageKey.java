package com.s3d.messagebus.kafka.message;

import com.s3d.messagebus.kafka.KafkaMessageKey;

public class PartitionedMessageKey extends KafkaMessageKey{
	public PartitionedMessageKey() {
	}
	
	private Boolean isFinished = false;
	
	public Boolean getIsFinished() {
		return isFinished;
	}
	public void setIsFinished(Boolean isFinished) {
		this.isFinished = isFinished;
	}

	/**
	 * 监听http response者所在topic
	 */
	private String responseTopic;
	/**
	 * 监听http response者的partition位置
	 */
	private Integer responsePartiton;
	
	public String getResponseTopic() {
		return responseTopic;
	}
	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}
	public Integer getResponsePartiton() {
		return responsePartiton;
	}
	public void setResponsePartiton(Integer responsePartiton) {
		this.responsePartiton = responsePartiton;
	}
	
	@Override
	public String toString() {
		return "PartitionedMessageKey [key=" + getId()
				+ ", responseTopic=" + responseTopic
				+ ", responsePartiton=" + responsePartiton
				+ ", isFinished=" + isFinished
				+ "]";
	}
}
