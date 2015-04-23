package se.sics.ms.util;

import org.javatuples.*;
import se.sics.ms.data.AddIndexEntry;
import se.sics.ms.data.IndexHashExchange;
import se.sics.ms.types.IndexHash;
import se.sics.p2ptoolbox.util.ProbabilitiesHelper;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.*;

/**
 * Tracker for the index exchange mechanism.
 * The main purpose is to keep track of the responses and also determining the nodes to contact
 * for fetching the index entry information. 
 *  
 *  
 * FIX: <b> Under Implementation. </b>
 * Created by babbarshaer on 2015-04-23.
 */
public class IndexExchangeTracker {
    
    Map<DecoratedAddress, Collection<IndexHash>> exchangeRoundHashCollection;
    UUID exchangeRoundId;
    private int exchangeRequestNumber;
    
    
    public IndexExchangeTracker(int exchangeRequestNumber){
        
        this.exchangeRoundHashCollection = new HashMap<DecoratedAddress, Collection<IndexHash>>();
        this.exchangeRequestNumber = exchangeRequestNumber;
    }
    
    
    public void startTracking(UUID exchangeRoundId){
        
        this.exchangeRoundId = exchangeRoundId;
        this.exchangeRoundHashCollection.clear();
    }
    
    
    public void resetTracker(){
        
        exchangeRoundId = null;
        exchangeRoundHashCollection.clear();
    }
    
    public void addIndexHashResponse(DecoratedAddress address, IndexHashExchange.Response responseContainer){
        
        if(exchangeRoundId != null && exchangeRoundId.equals(responseContainer.getExchangeRoundId())){
            exchangeRoundHashCollection.put(address, responseContainer.getIndexHashes());
        }
    }
    
    public boolean allResponsesComplete(){
        return exchangeRoundId != null && (exchangeRoundHashCollection.size() >= this.exchangeRequestNumber);
    }
    
    
    public UUID getExchangeRoundId(){
        return this.exchangeRoundId;
    }


    
    /**
     * Based on the collection of hashes, 
     * return the common hashes present in the collection.
     * 
     * @return Common Hash Collection
     */
    public Collection<IndexHash> getIntersectionHashes(){
        
        Collection<IndexHash> intersectionHashes = new ArrayList<IndexHash>();
        if(allResponsesComplete()){
            
            intersectionHashes  = exchangeRoundHashCollection.values().iterator().next();
            for(Collection<IndexHash> collection : exchangeRoundHashCollection.values()){
                intersectionHashes.retainAll(collection);
            }
        }
        return intersectionHashes;
    }


    /**
     * Use a softmax approach to determine the node to contact to.
     * @return NodeAddress
     */
    public DecoratedAddress getSoftMaxBasedNode() {
        
        DecoratedAddress result = null;
        if(exchangeRoundHashCollection != null && !exchangeRoundHashCollection.isEmpty()){
            
            List<DecoratedAddress> keyList = new ArrayList<DecoratedAddress>(exchangeRoundHashCollection.keySet());
            int index= ProbabilitiesHelper.getSoftMaxVal(keyList.size(), new Random(), 10);
            result = keyList.get(index);
        }
        
        return result;
    }
}
