package se.sics.ms.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.ProbabilitiesHelper;
import se.sics.ms.data.EntryHashExchange;
import se.sics.ms.types.EntryHash;

import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

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
    private long seed;
    private Random random;
    Map<Identifier, Pair<KAddress, List<EntryHash>>> exchangeRoundEntryHashCollection = new HashMap<>();



    public EntryExchangeTracker(int higherNodesCount, long seed) {

        this.seed = seed;
        this.random = new Random(this.seed);
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
    public void addEntryHashResponse(KAddress address, EntryHashExchange.Response response) {

        if (exchangeRoundId != null && exchangeRoundId.equals(response.getExchangeRoundId())) {
            exchangeRoundEntryHashCollection.put(address.getId(), Pair.with(address, response.getEntryHashes()));
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
    
    
    
    public List<EntryHash> getCommonEntryHashes(Collection<Pair<KAddress, List<EntryHash>>> entryHashesCollection){

        List<EntryHash> intersection = new ArrayList<>();

        if(!entryHashesCollection.isEmpty()){

            intersection = entryHashesCollection.iterator().next().getValue1();
            for (Pair<KAddress, List<EntryHash>> anEntryHashesCollection : entryHashesCollection) {
                intersection.retainAll(anEntryHashesCollection.getValue1());
            }
        }

        return intersection;
    }

    /**
     * Use a softmax approach to determine the node to contact to.
     *
     * @return NodeAddress
     */
    public KAddress getSoftMaxBasedNode() {

        KAddress result = null;
        if (exchangeRoundEntryHashCollection != null && !exchangeRoundEntryHashCollection.isEmpty()) {

            List<Identifier> keyList = new ArrayList<>(exchangeRoundEntryHashCollection.keySet());
            int index = ProbabilitiesHelper.getSoftMaxVal(keyList.size(), random, 10);
            Identifier id = keyList.get(index);
            result = exchangeRoundEntryHashCollection.get(id).getValue0();
        }

        return result;
    }


    public int getResponseMapSize() {
        return this.exchangeRoundEntryHashCollection.size();
    }


    public Map<Identifier, Pair<KAddress, List<EntryHash>>> getExchangeRoundEntryHashCollection() {
        return exchangeRoundEntryHashCollection;
    }

    // Create a Method for handling the responses from the direct leader pull mechanism.
}
