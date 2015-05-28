package se.sics.ms.util;

import org.javatuples.*;
import se.sics.ms.data.AddIndexEntry;
import se.sics.ms.data.EntryHashExchange;
import se.sics.ms.data.IndexHashExchange;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.EntryHash;
import se.sics.ms.types.IndexHash;
import se.sics.p2ptoolbox.util.ProbabilitiesHelper;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.*;

/**
 * Tracker for the index exchange mechanism.
 * The main purpose is to keep track of the responses and also determining the nodes to contact
 * for fetching the index entry information.
 * <p/>
 * <p/>
 * FIX: <b> Under Implementation. </b>
 * Created by babbarshaer on 2015-04-23.
 */
public class EntryExchangeTracker {

    UUID exchangeRoundId;
    private int higherNodesCount;
    private boolean hashRoundAnswered;

    Map<DecoratedAddress, Collection<EntryHash>> exchangeRoundEntryHashCollection;



    public EntryExchangeTracker(int higherNodesCount) {

        this.exchangeRoundEntryHashCollection = new HashMap<DecoratedAddress, Collection<EntryHash>>();
        this.higherNodesCount = higherNodesCount;
    }


    public int getHigherNodesCount() {
        return this.higherNodesCount;
    }


    /**
     * Start tracking a new exchange round in the system.
     * Reset the old tracking information to prevent from handling the
     * old responses.
     *
     * @param exchangeRoundId Exchange Round Information.
     */
    public void startTracking(UUID exchangeRoundId) {

        this.exchangeRoundId = exchangeRoundId;
        this.hashRoundAnswered = false;
        this.exchangeRoundEntryHashCollection.clear();
    }


    /**
     * Reset the tracking information to the default
     * value.
     */
    public void resetTracker() {

        exchangeRoundId = null;
        exchangeRoundEntryHashCollection.clear();
        this.hashRoundAnswered = false;
    }


    /**
     * Add the hash exchange response and based on the information contained in the response update
     * the already collected responses.
     *
     * @param address  Address
     * @param response response
     */
    public void addEntryHashResponse(DecoratedAddress address, EntryHashExchange.Response response) {

        if (exchangeRoundId != null && exchangeRoundId.equals(response.getExchangeRoundId())) {
            exchangeRoundEntryHashCollection.put(address, response.getEntryHashes());
        }
    }


    /**
     * Check if all the responses for the current hash exchange round has been received
     * and return the value.
     *
     * @return Responses Complete
     */
    public boolean allHashResponsesComplete() {
        return exchangeRoundId != null && (exchangeRoundEntryHashCollection.size() == this.higherNodesCount);
    }


    /**
     * Used ot calculate the majority vote in the system.
     * When majority of nodes have replied, then we move to commit phase.
     * The commit phase simply sends the message to all the higher
     * nodes the request to commit.
     *
     * @return
     */
    public boolean majorityResponses(){
        
        boolean result = false;
        
        if(exchangeRoundId != null && !hashRoundAnswered){
            
            if(exchangeRoundEntryHashCollection.size() 
                    >= Math.round((float)this.higherNodesCount/2)){
                
                result = true;
                hashRoundAnswered = true;
            }
        }
        
        return result;
    }
    
    
    /**
     * Fetch the current exchange round
     * information
     *
     * @return Exchange Round Information.
     */
    public UUID getExchangeRoundId() {
        return this.exchangeRoundId;
    }

    
    
    public boolean isHashRoundAnswered(){
        return this.isHashRoundAnswered();
    }
    
    
    
    public Collection<EntryHash> getCommonEntryHashes(Collection<Collection<EntryHash>> entryHashesCollection){

        Collection<EntryHash> intersection = new ArrayList<EntryHash>();

        if(!entryHashesCollection.isEmpty()){

            intersection = entryHashesCollection.iterator().next();
            for (Collection<EntryHash> anEntryHashesCollection : entryHashesCollection) {
                intersection.retainAll(anEntryHashesCollection);
            }
        }

        return intersection;


    }





    /**
     * Use a softmax approach to determine the node to contact to.
     *
     * @return NodeAddress
     */
    public DecoratedAddress getSoftMaxBasedNode() {

        DecoratedAddress result = null;
        if (exchangeRoundEntryHashCollection != null && !exchangeRoundEntryHashCollection.isEmpty()) {

            List<DecoratedAddress> keyList = new ArrayList<DecoratedAddress>(exchangeRoundEntryHashCollection.keySet());
            int index = ProbabilitiesHelper.getSoftMaxVal(keyList.size(), new Random(), 10);
            result = keyList.get(index);
        }

        return result;
    }


    public int getResponseMapSize() {
        return this.exchangeRoundEntryHashCollection.size();
    }


    public Map<DecoratedAddress, Collection<EntryHash>> getExchangeRoundEntryHashCollection() {
        return exchangeRoundEntryHashCollection;
    }

    // Create a Method for handling the responses from the direct leader pull mechanism.
}
