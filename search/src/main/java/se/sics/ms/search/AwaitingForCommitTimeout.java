package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:11 PM
 */
public class AwaitingForCommitTimeout extends Timeout {
    private final IndexEntry entry;

    public AwaitingForCommitTimeout(ScheduleTimeout rst, IndexEntry entry) {
        super(rst);

        this.entry = entry;
    }

    public IndexEntry getEntry() {
        return entry;
    }
}
