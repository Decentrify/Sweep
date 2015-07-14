package se.sics.ms.gradient.events;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * Timeout triggered when a node waiting for the 
 * verification request to be completed by the neighbouring nodes in the system.
 * 
 * @author babbarshaer
 */
public class AwaitVerificationTimeout extends Timeout{
    
    public AwaitVerificationTimeout(ScheduleTimeout request) {
        super(request);
    }
}
