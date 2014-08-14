package se.sics.ms.model;

import se.sics.gvod.timer.UUID;
import se.sics.ms.types.SearchPattern;

import java.util.HashSet;
import java.util.Set;

/**
 * Stores information about a currently executed search request.
 */
public class LocalSearchRequest {
    private final SearchPattern pattern;
    private Set<Integer> respondedPartitions = new HashSet<Integer>();
    private UUID timeoutId;

    /**
     * Create a new instance for the given request and query.
     *
     * @param pattern
     *            the pattern of the search
     */
    public LocalSearchRequest(SearchPattern pattern) {
        super();
        this.pattern = pattern;
    }

    /**
     * @return the pattern of the search
     */
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