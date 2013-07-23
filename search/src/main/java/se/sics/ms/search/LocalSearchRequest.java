package se.sics.ms.search;

import se.sics.gvod.timer.UUID;
import se.sics.kompics.web.WebRequest;
import se.sics.peersearch.types.SearchPattern;

/**
 * Stores information about a currently executed search request.
 */
public class LocalSearchRequest {
    private final SearchPattern pattern;
    private final UUID searchId;
    private int nodesQueried;
    private int nodesAnswered;
    private UUID timeoutId;

    /**
     * Craete a new instance for the given request and query.
     *
     * @param pattern
     *            the pattern of the search
     */
    public LocalSearchRequest(SearchPattern pattern) {
        super();
        this.pattern = pattern;
        // Create a unique id for each request
        this.searchId = (UUID)UUID.nextUUID();
    }

    /**
     * @return the pattern of the search
     */
    public SearchPattern getSearchPattern() {
        return pattern;
    }

    /**
     * @return the UUID of the search request
     */
    public UUID getSearchId() {
        return searchId;
    }

    /**
     * @return the amount of nodes queried
     */
    public int getNodesQueried() {
        return nodesQueried;
    }

    /**
     * @param sent
     *            the amount of nodes queried
     */
    public void setNodesQueried(int sent) {
        this.nodesQueried = sent;
    }

    /**
     * Increment the amount of nodes queried.
     */
    public void incrementNodesQueried() {
        this.nodesQueried++;
    }

    /**
     * @return the amount of answers received
     */
    public int getNodesAnswered() {
        return nodesAnswered;
    }

    /**
     * @param received
     *            the amount of answers received
     */
    public void setNodesAnswered(int received) {
        this.nodesAnswered = received;
    }

    /**
     * Increment the amount of answers received.
     */
    public void incrementReceived() {
        this.nodesAnswered++;
    }

    /**
     * @return true if the amount of answers matches the amount of requests sent
     */
    public boolean receivedAll() {
        return nodesQueried <= nodesAnswered;
    }

    /**
     * @param timeoutId
     *            the id of the timeout related to this search
     */
    public void setTimeoutId(UUID timeoutId) {
        this.timeoutId = timeoutId;
    }

    /**
     * @return the id of the timeout related to this search
     */
    public UUID getTimeoutId() {
        return timeoutId;
    }
}