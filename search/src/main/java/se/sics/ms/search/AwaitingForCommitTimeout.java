package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:11 PM
 */
public class AwaitingForCommitTimeout extends Timeout {
    private final int id;

    public AwaitingForCommitTimeout(ScheduleTimeout rst, int id) {
        super(rst);

        this.id = id;
    }

    public int getId() {
        return id;
    }
}
