package com.s3d.messagebus.kafka;

public class BrokerFailureException extends RuntimeException {
	
	private static final long serialVersionUID = 1309891778829246493L;
	public BrokerFailureException(String message) {
        super(message);
    }
	public BrokerFailureException(Throwable t) {
		super(t);
	}
}
