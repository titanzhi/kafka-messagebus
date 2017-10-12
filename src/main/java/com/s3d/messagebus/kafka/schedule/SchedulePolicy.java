package com.s3d.messagebus.kafka.schedule;

public interface SchedulePolicy {

	long fail(boolean shouldSleep);

	void succeess();

}
