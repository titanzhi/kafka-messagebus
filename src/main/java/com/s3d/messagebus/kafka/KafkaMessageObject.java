package com.s3d.messagebus.kafka;

/**
 * @author sulta
 */
public class KafkaMessageObject<K,V> {
	
	private K key;
	private V message;
	
	private int partition;
	private long offset;
	
	
	public KafkaMessageObject(K key,V message) {
		this.key = key;
		this.message = message;
	}
	public KafkaMessageObject(K key,V message , int partition, long offset) {
		this.key = key;
		this.message = message;
		this.partition = partition;
		this.offset = offset;
	}

	public K getKey() {
		return key;
	}

	public V getMessage() {
		return message;
	}
	
	public int getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}
	
	@Override
	public String toString() {
		return "KafkaMessageObject [key=" + key.toString() + ", message=" + message.toString() + "]";
	}
}
