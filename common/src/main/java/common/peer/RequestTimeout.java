package common.peer;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class RequestTimeout extends Timeout {

	public RequestTimeout(ScheduleTimeout request) {
		super(request);
	}
}
