package se.sics.ms.timeout;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.util.PartitionHelper;

/**
 *
 * Absence of Partition Commit Message Triggers the timeout.
 *
 * @author babbarshaer
 */
public class PartitionCommitTimeout extends Timeout {

    private final PartitionHelper.PartitionInfo partitionInfo;

    public PartitionCommitTimeout(ScheduleTimeout request , PartitionHelper.PartitionInfo partitionInfo) {
        super(request);
        this.partitionInfo = partitionInfo;
    }

    public PartitionHelper.PartitionInfo getPartitionInfo(){
        return this.partitionInfo;
    }
}
