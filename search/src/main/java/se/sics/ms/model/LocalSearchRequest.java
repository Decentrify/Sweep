package se.sics.ms.model;

import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.EntryScorePair;
import se.sics.ms.util.IdScorePair;
import se.sics.ms.util.PaginateInfo;

import java.util.*;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Stores information about a currently executed search request.
 */
public class LocalSearchRequest {

    private Set<Integer> respondedPartitions;
    private UUID searchRoundId;
    private QueryPhaseTracker queryPhaseTracker;
    private FetchPhaseTracker fetchPhaseTracker;
    private ClientResponseTracker clientResponseTracker;


    public LocalSearchRequest(){
        super();
    }


    /**
     * Create a new instance for the given request and query.
     *
     * @param pattern
     * the pattern of the search
     */
    public LocalSearchRequest(SearchPattern pattern) {
        super();
        this.respondedPartitions = new HashSet<Integer>();
    }

    public void startSearch (SearchPattern pattern, PaginateInfo paginateInfo, UUID searchRoundId) {

        this.searchRoundId = searchRoundId;
        this.queryPhaseTracker = new QueryPhaseTracker();
        this.fetchPhaseTracker = new FetchPhaseTracker();
        this.clientResponseTracker = new ClientResponseTracker(pattern, paginateInfo);

    }


    @Override
    public String toString() {
        return "LocalSearchRequest{" +
                "respondedPartitions=" + respondedPartitions +
                ", searchRoundId=" + searchRoundId +
                ", queryPhaseTracker=" + queryPhaseTracker +
                ", fetchPhaseTracker=" + fetchPhaseTracker +
                ", clientResponseTracker=" + clientResponseTracker +
                '}';
    }

    /**
     * Check if it is safe to add the partition to the
     * partitions that have already responded. Never incorporate the
     * responses from the
     *
     * @return
     */
    public boolean isSafeToAdd(int partition){
        return (this.queryPhaseTracker != null) && this.queryPhaseTracker.isSafeToAdd(partition);
    }


    /**
     * The application needs to check that whether all the shards have responded
     * and that the application is safe to continue forward.
     *
     * @return all shards replied
     */
    public boolean haveAllShardsResponded(){
        return this.queryPhaseTracker != null && this.queryPhaseTracker.haveAllShardsResponded();
    }


    /**
     * Update the map with the information about the
     * id score pairs in the system.
     *
     * @param address address
     * @param idScorePairs id score collection
     */
    public void storeIdScoreCollection(KAddress address, List<IdScorePair> idScorePairs){

        if(this.queryPhaseTracker != null){
            this.queryPhaseTracker.storeIdScoreCollection(address, idScorePairs);
        }
    }


    /**
     * Initiate the tracker for the fetch phase to keep track of the
     * responses.
     * @param fetchMap fetch phase map.
     */
    public void initiateFetchPhase(Map<Identifier, Pair<KAddress, List<IdScorePair>>> fetchMap) {

        if(this.fetchPhaseTracker != null){
            this.fetchPhaseTracker.initiateFetchPhase(fetchMap);
        }
    }


    /**
     * Capture and store the response during the fetch phase.
     * @param source source
     * @param entries entries
     */
    public void addFetchPhaseResponse(KAddress source, List<EntryScorePair> entries){

        if(this.fetchPhaseTracker != null){
            this.fetchPhaseTracker.addFetchPhaseResponse(source, entries);
        }
    }

    /**
     * Wrapper over the fetch tracker indicating the safety
     * for the application to respond back to the client.
     *
     * @return safety
     */
    public boolean isSafeToRespond(){
        return this.fetchPhaseTracker != null && this.fetchPhaseTracker.isSafeToReply();
    }

    /**
     * Getter for the collection of the entries that are replied back
     * by nodes during the search fetch phase.
     *
     * @return Fetched Entries.
     */
    public List<EntryScorePair> getFetchedEntries(){

        List<EntryScorePair> result = new ArrayList<>();
        if(this.fetchPhaseTracker != null) {
            for(Pair<KAddress, List<EntryScorePair>> entries : this.fetchPhaseTracker.fetchedPhaseResponseMap.values()){
                result.addAll(entries.getValue1());
            }
        }

        return result;
    }

    public Map<Identifier, Pair<KAddress, List<IdScorePair>>> getIdScoreMap() {
        return this.queryPhaseTracker != null ? this.queryPhaseTracker.idScoreMap : null;
    }


    /**
     * Store the number of hits for the search query that was sent by the
     * client to the application.
     *
     * @param numHits
     */
    public void storeNumHits(int numHits) {

        if(this.clientResponseTracker != null){
            this.clientResponseTracker.addNumHits(numHits);
        }
    }

    /**
     * Store the response which needs to be sent to the client back.
     *
     * @param entries
     */
    public void addClientResponse(List<ApplicationEntry> entries){

        if(this.clientResponseTracker != null){
            this.clientResponseTracker.addResponse(entries);
        }
    }


    /**
     * Accessor for the search pattern sent by the client to the
     * application for the search to be performed.
     *
     * @return
     */
    public SearchPattern getSearchPattern() {
        return this.clientResponseTracker != null ? this.clientResponseTracker.getPattern() : null;
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
        return this.clientResponseTracker != null ? clientResponseTracker.paginateInfo : null;
    }

    public java.util.UUID getSearchRoundId() {
        return searchRoundId;
    }

    public int getNumberOfShards(){
        return this.queryPhaseTracker != null ? this.queryPhaseTracker.numberOfShards : 0;
    }

    public void setNumberOfShards(int numberOfShards){

        if(this.queryPhaseTracker != null ){
            this.queryPhaseTracker.setNumberOfShards(numberOfShards);
        }
    }

    public void wipeExistingRequest(){

        this.searchRoundId = null;
        this.fetchPhaseTracker = null;
        this.queryPhaseTracker = null;
        this.clientResponseTracker = null;
    }

    public int getNumHits() {

        int result = -2;

        if(this.clientResponseTracker != null){
            result = clientResponseTracker.numHits;
        }

        return result;
    }


    /**
     * ************
     * QUERY PHASE TRACKER
     * ************
     */
    private class QueryPhaseTracker{

        private int numberOfShards;
        private Set<Integer> respondedPartitions = new HashSet<>();
        private Map<Identifier, Pair<KAddress, List<IdScorePair>>> idScoreMap = new HashMap<>();

        public QueryPhaseTracker(){
            this.numberOfShards = 0;
        }


        @Override
        public String toString() {
            return "QueryPhaseTracker{" +
                    "numberOfShards=" + numberOfShards +
                    ", respondedPartitions=" + respondedPartitions +
                    ", idScoreMap=" + idScoreMap +
                    '}';
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
         * Set the number of shards in the system.
         * @param numberOfShards number of shards.
         */
        public void setNumberOfShards(int numberOfShards){
            this.numberOfShards = numberOfShards;
        }

        /**
         * Check if it is safe to add the partition to the
         * partitions that have already responded. Never incorporate the
         * responses from the
         *
         * @return true - if safe
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
         * Update the map with the information about the
         * id score pairs in the system.
         *
         * @param address address
         * @param idScorePairs id score collection
         */
        public void storeIdScoreCollection(KAddress address, List<IdScorePair> idScorePairs){
            idScoreMap.put(address.getId(), Pair.with(address, idScorePairs));
        }
    }

    /**
     * ************
     * FETCH PHASE TRACKER
     * ************
     */
    private class FetchPhaseTracker {


        public Map<Identifier, Pair<KAddress, List<IdScorePair>>> fetchPhaseRequestMap = new HashMap<>();
        public Map<Identifier, Pair<KAddress, List<EntryScorePair>>> fetchedPhaseResponseMap = new HashMap<>();

        public FetchPhaseTracker(){
        }

        public void initiateFetchPhase(Map<Identifier, Pair<KAddress, List<IdScorePair>>> fetchPhaseRequestMap){
            this.fetchPhaseRequestMap = fetchPhaseRequestMap;
        }


        @Override
        public String toString() {
            return "FetchPhaseTracker{" +
                    "fetchPhaseRequestMap=" + fetchPhaseRequestMap +
                    ", fetchedPhaseResponseMap=" + fetchedPhaseResponseMap +
                    '}';
        }

        /**
         * Initiate the fetch the phase for the entries in the
         * system.
         *
         * @return Map.
         */
        public Map<Identifier, Pair<KAddress, List<IdScorePair>>> getFetchPhaseMap() {
            return this.fetchPhaseRequestMap;
        }

        /**
         * Store the fetch phase response from a particular node.
         * Check for the address from which the response is received and it should be
         * contained in the original map using which the request was made.
         *
         * @param source source
         * @param entries application entries
         */
        public void addFetchPhaseResponse(KAddress source, List<EntryScorePair>entries){

            if(fetchPhaseRequestMap.containsKey(source.getId())) {
                fetchedPhaseResponseMap.put(source.getId(), Pair.with(source, entries));
            }
        }

        /**
         * Before a node can move ahead with final response to the Clinet, on every
         * response from the node belonging to the fetch phase, it has to check for the
         * completeness of the responses.
         *
         * @return true - if safe to reply.
         */
        public boolean isSafeToReply() {
            return (fetchedPhaseResponseMap.size() >= fetchPhaseRequestMap.size());
        }
    }


    /**
     * ************
     * CLIENT RESPONSE TRACKER
     * ************
     */
    private class ClientResponseTracker {

        public int numHits;
        public List<ApplicationEntry> applicationEntries;
        public SearchPattern pattern;
        public PaginateInfo paginateInfo;

        public ClientResponseTracker(SearchPattern pattern, PaginateInfo paginateInfo){

            this.pattern = pattern;
            this.paginateInfo = paginateInfo;
            this.numHits = 0;
            this.applicationEntries = new ArrayList<ApplicationEntry>();
        }

        @Override
        public String toString() {
            return "ClientResponseTracker{" +
                    "numHits=" + numHits +
                    ", applicationEntries=" + applicationEntries +
                    ", pattern=" + pattern +
                    ", paginateInfo=" + paginateInfo +
                    '}';
        }

        public SearchPattern getPattern(){
            return this.pattern;
        }

        public void addNumHits(int hits){
            this.numHits = hits;
        }

        public void addResponse(List<ApplicationEntry> entries){
            this.applicationEntries = entries;
        }

    }


}