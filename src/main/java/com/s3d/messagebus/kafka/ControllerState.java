package com.s3d.messagebus.kafka;

public enum ControllerState {
	
	/**
	 * 未启动
	 */
	Not_Run,
	/**
	 * 已启动
	 */
	Started,
	/**
	 * 终止
	 */
	Terminated,
	/**
	 * 关闭
	 */
	Shutdown;
	
}
