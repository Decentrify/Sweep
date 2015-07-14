package se.sics.ms.gradient.events;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * Periodic timeout by PAG to inform the application about the 
 * network partitioned nodes in the system.
 *
 * Created by babbarshaer on 2015-06-07.
 */
public class NPTimeout extends Timeout{

    public NPTimeout(SchedulePeriodicTimeout request) {
        super(request);
    }
}