package se.sics.ms.events.simEvents;

import se.sics.ms.types.SearchPattern;

/**
 * Simulation event for searching in the system.
 *
 * Created by babbarshaer on 2015-04-25.
 */
public class SearchP2pSimulated {
    
    private final SearchPattern searchPattern;
    
    public SearchP2pSimulated(SearchPattern pattern){
        this.searchPattern = pattern;
    }

    @Override
    public boolean equals(Object o) {
        
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchP2pSimulated that = (SearchP2pSimulated) o;

        if (searchPattern != null ? !searchPattern.equals(that.searchPattern) : that.searchPattern != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return searchPattern != null ? searchPattern.hashCode() : 0;
    }

    public SearchPattern getSearchPattern() {
        return searchPattern;
    }
}
