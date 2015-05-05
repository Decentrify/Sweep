package se.sics.ms.timeout;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:11 PM
 */
public class AwaitingForCommitTimeout extends Timeout {

    private  IndexEntry entry;
    private  ApplicationEntry applicationEntry;
    public AwaitingForCommitTimeout(ScheduleTimeout rst, IndexEntry entry) {
        super(rst);
        this.entry = entry;
    }

    public AwaitingForCommitTimeout(ScheduleTimeout rst, ApplicationEntry applicationEntry) {
        super(rst);
        this.applicationEntry = applicationEntry;
    }

    public ApplicationEntry getApplicationEntry () {
        return this.applicationEntry;
    }

    public IndexEntry getEntry() {
        return entry;
    }
}
