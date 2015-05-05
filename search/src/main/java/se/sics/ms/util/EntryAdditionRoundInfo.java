package se.sics.ms.util;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.data.EntryAddPrepare;
import se.sics.ms.data.ReplicationPrepareCommit;
import se.sics.ms.messages.ReplicationPrepareCommitMessage;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.Collection;
import java.util.UUID;

/**
 * Simple POJO containing the information for a entry addition round.
 *  
 * Created by babbarshaer on 2015-04-13.
 */
public class EntryAdditionRoundInfo {
    
    private Collection<DecoratedAddress> leaderGroupAddress;
    private int promiseResponses;
    private IndexEntry entryToAdd;
    private DecoratedAddress entryAddSourceNode;
    private UUID entryAdditionRoundId;
    private ApplicationEntry applicationEntry;
    
    public EntryAdditionRoundInfo(UUID entryAdditionRoundId, Collection<DecoratedAddress> leaderGroupAddress, IndexEntry entry, DecoratedAddress entryAddSourceNode){

        this.entryAdditionRoundId = entryAdditionRoundId;
        this.leaderGroupAddress = leaderGroupAddress;
        this.promiseResponses = 0;
        this.entryToAdd = entry;
        this.entryAddSourceNode = entryAddSourceNode;
    }

    public EntryAdditionRoundInfo(UUID entryAdditionRoundId, Collection<DecoratedAddress>leaderGroupAddress, ApplicationEntry applicationEntry, DecoratedAddress entryAddSourceNode){
        this.entryAdditionRoundId = entryAdditionRoundId;
        this.leaderGroupAddress = leaderGroupAddress;
        this.promiseResponses = 0;
        this.applicationEntry = applicationEntry;
        this.entryAddSourceNode = entryAddSourceNode;
    }


    public void addEntryAddPromiseResponse(EntryAddPrepare.Response response){
        
        if(entryAdditionRoundId != null && response.getEntryAdditionRound().equals(entryAdditionRoundId)){
            promiseResponses +=1;
        }
    }
    
    public boolean isPromiseAccepted(){
        return this.promiseResponses >= this.leaderGroupAddress.size();
    }
    
    public Collection<DecoratedAddress> getLeaderGroupAddress() {
        return leaderGroupAddress;
    }

    public IndexEntry getEntryToAdd() {
        return entryToAdd;
    }

    public ApplicationEntry getApplicationEntry(){
        return this.applicationEntry;
    }

    public DecoratedAddress getEntryAddSourceNode() {
        return entryAddSourceNode;
    }
    
    public UUID getEntryAdditionRoundId(){
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
