package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:31 PM
 */
public class CommitTimeout extends Timeout {
    private final int id;
    public CommitTimeout(ScheduleTimeout rst, int id) {
        super(rst);

        this.id = id;
    }
}
