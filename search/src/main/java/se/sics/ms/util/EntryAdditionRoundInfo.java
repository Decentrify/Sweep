package se.sics.ms.util;

import se.sics.ms.data.EntryAddPrepare;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.LeaderUnit;
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
    private LeaderUnit previousEpochUpdate;
    
    public EntryAdditionRoundInfo(UUID entryAdditionRoundId, Collection<DecoratedAddress> leaderGroupAddress, IndexEntry entry, DecoratedAddress entryAddSourceNode){

        this.entryAdditionRoundId = entryAdditionRoundId;
        this.leaderGroupAddress = leaderGroupAddress;
        this.promiseResponses = 0;
        this.entryToAdd = entry;
        this.entryAddSourceNode = entryAddSourceNode;
    }

    public EntryAdditionRoundInfo(UUID entryAdditionRoundId, Collection<DecoratedAddress>leaderGroupAddress, ApplicationEntry applicationEntry, DecoratedAddress entryAddSourceNode, LeaderUnit previousEpochUpdate){
        
        this.entryAdditionRoundId = entryAdditionRoundId;
        this.leaderGroupAddress = leaderGroupAddress;
        this.promiseResponses = 0;
        this.applicationEntry = applicationEntry;
        this.entryAddSourceNode = entryAddSourceNode;
        this.previousEpochUpdate = previousEpochUpdate;
    }


    public void addEntryAddPromiseResponse(EntryAddPrepare.Response response){
        
        if(entryAdditionRoundId != null
                && response.getEntryAdditionRound().equals(entryAdditionRoundId)
                && promiseResponses < Math.round((float)this.leaderGroupAddress.size()/2)){

            promiseResponses +=1;
        }
    }

    /**
     * In a distributed system, a leader should not wait
     * for all the response but only wait till majority are
     * received.
     *
     * @deprecated
     *
     * @return All Promised.
     */
    public boolean isPromiseAccepted(){
        return this.promiseResponses >= this.leaderGroupAddress.size();
    }


    /**
     * Used ot calculate the majority vote in the system.
     * When majority of nodes have replied, then we move to commit phase.
     * The commit phase simply sends the message to all the higher
     * nodes the request to commit.
     *
     * @return
     */
    public boolean isPromiseMajority(){

        boolean result = false;

        if( (promiseResponses >=  Math.round((float)this.leaderGroupAddress.size()/2))
                && entryAdditionRoundId != null) {
            result = true;
        }

        return result;
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

    public LeaderUnit getAssociatedEpochUpdate() {
        return this.previousEpochUpdate;
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
