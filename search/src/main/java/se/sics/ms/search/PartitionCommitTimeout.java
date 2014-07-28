package se.sics.ms.search;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.util.PartitionHelper;

/**
 *
 * Absence of Partition Commit Message Triggers the timeout.
 *
 * @author babbarshaer
 */
public class PartitionCommitTimeout extends IndividualTimeout {

    private final PartitionHelper.PartitionInfo partitionInfo;

    public PartitionCommitTimeout(ScheduleTimeout request, int id , PartitionHelper.PartitionInfo partitionInfo) {
        super(request, id);
        this.partitionInfo = partitionInfo;
    }

    public PartitionHelper.PartitionInfo getPartitionInfo(){
        return this.partitionInfo;
    }
}
