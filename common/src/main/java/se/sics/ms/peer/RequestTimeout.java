package se.sics.ms.peer;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;

public class RequestTimeout extends Timeout {

	public RequestTimeout(ScheduleTimeout request) {
		super(request);
	}
}
