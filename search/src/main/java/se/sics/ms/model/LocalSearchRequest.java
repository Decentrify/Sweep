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
    private FetchPhaseTracker fetchPhaseTracker;
    private ClientResponseTracker clientResponseTracker;

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
        this.fetchPhaseTracker = new FetchPhaseTracker();
        this.clientResponseTracker = new ClientResponseTracker(pattern);

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


    /**
     * Initiate the tracker for the fetch phase to keep track of the
     * responses.
     * @param fetchMap fetch phase map.
     */
    public void initiateFetchPhase(Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchMap) {

        if(this.fetchPhaseTracker != null){
            this.fetchPhaseTracker.initiateFetchPhase(fetchMap);
        }
    }


    /**
     * Capture and store the response during the fetch phase.
     * @param source source
     * @param entries entries
     */
    public void addFetchPhaseResponse(DecoratedAddress source, Collection<ApplicationEntry> entries){

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

        boolean result = false;

        if(this.fetchPhaseTracker != null ){
            result= this.fetchPhaseTracker.isSafeToReply();
        }

        return result;
    }

    /**
     * Getter for the collection of the entries that are replied back
     * by nodes during the search fetch phase.
     *
     * @return Fetched Entries.
     */
    public List<ApplicationEntry> getFetchedEntries(){

        List<ApplicationEntry> result = new ArrayList<ApplicationEntry>();
        if(this.fetchPhaseTracker != null) {
            for(Collection<ApplicationEntry> entries : this.fetchPhaseTracker.fetchedPhaseResponseMap.values()){
                result.addAll(entries);
            }
        }

        return result;
    }

    public Map<DecoratedAddress, Collection<IdScorePair>> getIdScoreMap() {
        return idScoreMap;
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

        SearchPattern result = null;

        if(this.clientResponseTracker != null){
            result = clientResponseTracker.getPattern();
        }
        return result;
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
        this.pattern = null;
        this.numberOfShards = 0;
        this.idScoreMap = null;
        this.fetchPhaseTracker = null;
    }

    public int getNumHits() {

        int result = 0;

        if(this.clientResponseTracker != null){
            result = clientResponseTracker.numHits;
        }

        return result;
    }


    private class FetchPhaseTracker {


        public Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchPhaseRequestMap;
        public Map<DecoratedAddress, Collection<ApplicationEntry>> fetchedPhaseResponseMap;

        public FetchPhaseTracker(){
            this.fetchPhaseRequestMap = new HashMap<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>>();
            this.fetchedPhaseResponseMap = new HashMap<DecoratedAddress, Collection<ApplicationEntry>>();
        }

        public void initiateFetchPhase(Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchPhaseRequestMap){
            this.fetchPhaseRequestMap = fetchPhaseRequestMap;
        }

        /**
         * Initiate the fetch the phase for the entries in the
         * system.
         *
         * @return Map.
         */
        public Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> getFetchPhaseMap() {
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
        public void addFetchPhaseResponse(DecoratedAddress source, Collection<ApplicationEntry>entries){

            if(fetchPhaseRequestMap.containsKey(source)) {
                fetchedPhaseResponseMap.put(source, entries);
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


    private class ClientResponseTracker {

        public int numHits;
        public List<ApplicationEntry> applicationEntries;
        public SearchPattern pattern;

        public ClientResponseTracker(SearchPattern pattern){

            this.pattern = pattern;
            this.numHits = 0;
            this.applicationEntries = new ArrayList<ApplicationEntry>();
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