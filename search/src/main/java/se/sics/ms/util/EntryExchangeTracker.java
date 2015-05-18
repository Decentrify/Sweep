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
     * Fetch the current exchange round
     * information
     *
     * @return Exchange Round Information.
     */
    public UUID getExchangeRoundId() {
        return this.exchangeRoundId;
    }

    /**
     * Based on the collection of hashes,
     * return the common hashes present in the collection.
     *
     * @return Common Hash Collection
     */
    public Collection<IndexHash> getIntersectionHashes() {
        throw new UnsupportedOperationException("Operation Unsupported");
    }


    /**
     * The application needs to always add entries in the system in order.
     * Therefore, when we get the responses from the nodes in terms of the hashes we need to
     * process the hashes based on the current missing entry.
     *
     * @return EntryHash Collection.
     */
    public Collection<EntryHash> getInOrderEntryHashes(ApplicationEntry.ApplicationEntryId currentMissingId) {

        Collection<EntryHash> commonInOrderCollection = new ArrayList<EntryHash>();

        if (allHashResponsesComplete()) {

            Collection<EntryHash> intersection =
                    new HashSet<EntryHash>(exchangeRoundEntryHashCollection.values().iterator().next());

            for (Collection<EntryHash> hashCollection : exchangeRoundEntryHashCollection.values()) {
                intersection.retainAll(hashCollection);
            }

            if (!intersection.isEmpty()) {

                // Check the ordering now.
                boolean entryHashFound = true;

                while (entryHashFound) {

                    entryHashFound = false;
                    for (EntryHash hash : intersection) {

                        if (hash.getEntryId().equals(currentMissingId)) {

                            commonInOrderCollection.add(hash);
                            currentMissingId = new ApplicationEntry.ApplicationEntryId(
                                    currentMissingId.getEpochId(),
                                    currentMissingId.getLeaderId(),
                                    currentMissingId.getEpochId() + 1);

                            entryHashFound = true;
                            break;
                        }
                    }
                }


            }
        }

        return commonInOrderCollection;
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
