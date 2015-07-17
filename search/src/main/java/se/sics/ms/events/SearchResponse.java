package se.sics.ms.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PaginateInfo;

import java.util.List;

/**
 *
 * Event indicating the response from the server for the
 * search entries with the pagination information.
 *
 */
public class SearchResponse implements KompicsEvent {

    private List<ApplicationEntry> results;
    private int totalHits;
    private PaginateInfo paginateInfo;
    private SearchPattern pattern;

    public SearchResponse (List<ApplicationEntry> result, int totalHits, PaginateInfo paginateInfo, SearchPattern pattern){

        this.pattern = pattern;
        this.results = result;
        this.totalHits = totalHits;
        this.paginateInfo = paginateInfo;
    }


    @Override
    public String toString() {
        return "SearchResponse{" +
                "results=" + results +
                ", totalHits=" + totalHits +
                ", paginateInfo=" + paginateInfo +
                ", pattern=" + pattern +
                '}';
    }

    public SearchPattern getPattern() {
        return pattern;
    }

    public List<ApplicationEntry> getResults() {
        return results;
    }

    public int getTotalHits(){
        return this.totalHits;
    }

    public PaginateInfo getPaginateInfo(){
        return this.paginateInfo;
    }
}
