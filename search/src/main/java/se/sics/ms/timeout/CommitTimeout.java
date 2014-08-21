package se.sics.ms.timeout;

import se.sics.gvod.timer.ScheduleTimeout;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:31 PM
 */
public class CommitTimeout extends IndividualTimeout {
    public CommitTimeout(ScheduleTimeout rst, int id) {
        super(rst, id);
    }
}
