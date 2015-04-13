package se.sics.ms.util;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.messages.ReplicationPrepareCommitMessage;
import se.sics.ms.types.IndexEntry;

import java.util.Collection;

/**
 * Simple POJO containing the information for a entry addition round.
 *  
 * Created by babbarshaer on 2015-04-13.
 */
public class EntryAdditionRoundInfo {
    
    private Collection<VodAddress> leaderGroupAddress;
    private int promiseResponses;
    private IndexEntry entryToAdd;
    private VodAddress entryAddSourceNode;
    private TimeoutId entryAdditionRoundId;
    
    public EntryAdditionRoundInfo(TimeoutId entryAdditionRoundId, Collection<VodAddress> leaderGroupAddress, IndexEntry entry, VodAddress entryAddSourceNode){
        this.entryAdditionRoundId = entryAdditionRoundId;
        this.leaderGroupAddress = leaderGroupAddress;
        this.promiseResponses = 0;
        this.entryToAdd = entry;
        this.entryAddSourceNode = entryAddSourceNode;
    }


    public void addEntryAddPromiseResponse(ReplicationPrepareCommitMessage.Response response){
        
        if(entryAdditionRoundId != null && response.getTimeoutId().equals(entryAdditionRoundId)){
            promiseResponses +=1;
        }
    }
    
    public boolean isPromiseAccepted(){
        return this.promiseResponses >= this.leaderGroupAddress.size();
    }
    
    public Collection<VodAddress> getLeaderGroupAddress() {
        return leaderGroupAddress;
    }

    public IndexEntry getEntryToAdd() {
        return entryToAdd;
    }

    public VodAddress getEntryAddSourceNode() {
        return entryAddSourceNode;
    }
    
    public TimeoutId getEntryAdditionRoundId(){
        return this.entryAdditionRoundId;
    }
    
    @Override
    public String toString(){
        
        StringBuilder builder = new StringBuilder();
        builder.append("entryAdditionRoundId: ").append(entryAdditionRoundId).append("\n")
                .append("promiseResponses: ").append(promiseResponses).append("\n")
                .append("entryToAdd: ").append(entryToAdd.getId()).append("\n");
        return builder.toString();
    }
    
}
