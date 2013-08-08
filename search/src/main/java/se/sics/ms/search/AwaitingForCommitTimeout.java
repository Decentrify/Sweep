package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/3/13
 * Time: 2:11 PM
 */
public class AwaitingForCommitTimeout extends IndividualTimeout {
    private final IndexEntry entry;

    public AwaitingForCommitTimeout(ScheduleTimeout rst, int id, IndexEntry entry) {
        super(rst, id);

        this.entry = entry;
    }

    public IndexEntry getEntry() {
        return entry;
    }
}
