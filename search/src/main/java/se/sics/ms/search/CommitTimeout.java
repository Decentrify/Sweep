package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.ms.timeout.IndividualTimeout;

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
