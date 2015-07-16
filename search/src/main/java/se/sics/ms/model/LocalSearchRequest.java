package se.sics.ms.model;

import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.IdScorePair;
import se.sics.ms.util.PaginateInfo;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.*;

/**
 * Stores information about a currently executed search request.
 */
public class LocalSearchRequest {

    private SearchPattern pattern;
    private Set<Integer> respondedPartitions;
    private UUID searchRoundId;
    private PaginateInfo paginateInfo;
    private int numberOfShards;
    private Map<DecoratedAddress, Collection<IdScorePair>> idScoreMap;
    /**
     * Create a new instance for the given request and query.
     *
     * @param pattern
     * the pattern of the search
     */
    public LocalSearchRequest(SearchPattern pattern) {
        super();
        this.pattern = pattern;
        this.respondedPartitions = new HashSet<Integer>();
    }

    public void startSearch (SearchPattern pattern, PaginateInfo paginateInfo, UUID searchRoundId) {

        this.searchRoundId = searchRoundId;
        this.pattern = pattern;
        this.paginateInfo = paginateInfo;
        this.respondedPartitions = new HashSet<Integer>();
        this.idScoreMap = new HashMap<DecoratedAddress, Collection<IdScorePair>>();
        this.numberOfShards = 0;
    }


    /**
     * Check if it is safe to add the partition to the
     * partitions that have already responded. Never incorporate the
     * responses from the
     *
     * @return
     */
    public boolean isSafeToAdd(int partition){

        boolean safety = false;
        if(numberOfShards != 0
                && numberOfShards > respondedPartitions.size()){

            if(!respondedPartitions.contains(partition)) {

                respondedPartitions.add(partition);
                safety = true;
            }
        }

        return safety;
    }


    /**
     * The application needs to check that whether all the shards have responded
     * and that the application is safe to continue forward.
     *
     * @return all shards replied
     */
    public boolean haveAllShardsResponded(){
        return (numberOfShards  !=0 && respondedPartitions.size() >= numberOfShards);
    }


    /**
     * Update the map with the information about the
     * id score pairs in the system.
     *
     * @param address address
     * @param idScorePairs id score collection
     */
    public void storeIdScoreCollection(DecoratedAddress address, Collection<IdScorePair> idScorePairs){
        idScoreMap.put(address, idScorePairs);
    }


    public Map<DecoratedAddress, Collection<IdScorePair>> getIdScoreMap() {
        return idScoreMap;
    }

    public SearchPattern getSearchPattern() {
        return pattern;
    }

    public void addRespondedPartition(int partition) {
        respondedPartitions.add(partition);
    }

    public boolean hasResponded(int partition) {
        return respondedPartitions.contains(partition);
    }

    public int getNumberOfRespondedPartitions() {
        return respondedPartitions.size();
    }

    public void setSearchRoundId(java.util.UUID searchRoundId) {
        this.searchRoundId = searchRoundId;
    }

    public PaginateInfo getPaginateInfo() {
        return paginateInfo;
    }

    public java.util.UUID getSearchRoundId() {
        return searchRoundId;
    }

    public int getNumberOfShards(){
        return this.numberOfShards;
    }

    public void setNumberOfShards(int numberOfShards){
        this.numberOfShards = numberOfShards;
    }

    public void wipeExistingRequest(){

        this.searchRoundId = null;
        this.respondedPartitions = null;
        this.paginateInfo = null;
        this.pattern = null;
        this.numberOfShards = 0;
        this.idScoreMap = null;
    }



    private class FetchPhaseTracker {

        private int fetchedResponses;
        private Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchPhaseRequestMap;
        private Map<DecoratedAddress, List<ApplicationEntry>> fetchPhaseResponseMap;

        public FetchPhaseTracker(int fetchedResponses, Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchPhaseRequestMap){

            this.fetchedResponses = fetchedResponses;
            this.fetchPhaseRequestMap = fetchPhaseRequestMap;
        }

        public int getFetchedResponses() {
            return this.fetchedResponses;
        }

        public Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> getFetchPhaseMap() {
            return this.fetchPhaseRequestMap;
        }


        public void addFetchPhaseResponse(DecoratedAddress source, List<ApplicationEntry>entries){

        }

        public boolean isSafeToReply() {
            return (fetchPhaseResponseMap.size() >= fetchPhaseRequestMap.size());
        }
    }




}