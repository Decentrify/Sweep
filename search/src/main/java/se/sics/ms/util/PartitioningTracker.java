package se.sics.ms.util;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.messages.PartitionCommitMessage;
import se.sics.ms.messages.PartitionPrepareMessage;

import java.util.Collection;

/**
 * This is the tracker for the partitioning information update.
 *
 * Created by babbar on 2015-04-13.
 */
public class PartitioningTracker {

    public TimeoutId partitionRequestId;
    public Collection<VodAddress> leaderGroupNodes;
    int promises;
    int commits;
    PartitionHelper.PartitionInfo partitionInfo;

    public PartitioningTracker(){

    }

    public void startTracking(TimeoutId partitionRequestId , Collection<VodAddress> leaderGroupNodes, PartitionHelper.PartitionInfo partitionInfo){

        this.partitionRequestId = partitionRequestId;
        this.leaderGroupNodes = leaderGroupNodes;
        this.partitionInfo = partitionInfo;
        this.promises = 0;
        this.commits = 0;

    }

    public void addPromiseResponse(PartitionPrepareMessage.Response response){

        if(partitionRequestId != null && response.getPartitionRequestId().equals(partitionRequestId)){
            promises++;
        }
    }

    public void addCommitResponse(PartitionCommitMessage.Response response){

        if(partitionRequestId != null && response.getPartitionRequestId().equals(partitionRequestId)){
            commits++;
        }
    }

    public boolean isPromiseAccepted(){
        return (partitionRequestId!= null && (promises >= leaderGroupNodes.size()));
    }

    public boolean isCommitAccepted(){
        return (partitionRequestId!= null && (commits >= leaderGroupNodes.size()));
    }

    public void resetTracker(){

        this.partitionRequestId = null;
        this.leaderGroupNodes = null;
        this.promises = 0;
        this.partitionInfo = null;
        this.commits = 0;
    }

    public Collection<VodAddress> getLeaderGroupNodes(){
        return this.leaderGroupNodes;
    }

    public PartitionHelper.PartitionInfo getPartitionInfo(){
        return this.partitionInfo;
    }

    public TimeoutId getPartitionRequestId(){
        return this.partitionRequestId;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(" PartitionRoundId ").append(partitionRequestId).append("\n")
                .append(" isPromiseAccepted ").append(isPromiseAccepted()).append("\n")
                .append(" isCommitAccepted ").append(isCommitAccepted()).append("\n")
                .append(" LeaderGroupNodesSize ").append(getLeaderGroupNodes().size());
        return builder.toString();
    }
}
