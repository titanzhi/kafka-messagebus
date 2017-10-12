package com.s3d.messagebus.kafka;


public class ControllerException extends Exception {

    private static final long serialVersionUID = -5246721574363390235L;
    
    private ErrorCode errorCode;
    
    public ControllerException(ErrorCode errorCode) {
    	this.errorCode = errorCode;
    }
    
    public ErrorCode getErrorCode() {
		return errorCode;
	}

	public static enum ErrorCode {
    	
    	/**
    	 * 控制器不存在
    	 */
    	Controller_Not_Found,
    	
    	/**
    	 * 控制器已启动
    	 */
    	Controller_Hasbean_Started;
    	
    }
    
}
