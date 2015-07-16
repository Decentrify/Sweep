package se.sics.ms.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.util.PaginateInfo;

import java.util.ArrayList;

/**
 *
 * Event indicating the response from the server for the
 * search entries with the pagination information.
 *
 */
public class SearchResponse implements KompicsEvent {

    private ArrayList<ApplicationEntry> results;
    private int totalHits;
    private PaginateInfo paginateInfo;

    public SearchResponse (ArrayList<ApplicationEntry> result, int totalHits, PaginateInfo paginateInfo){
        this.results = result;
        this.totalHits = totalHits;
        this.paginateInfo = paginateInfo;
    }

    public ArrayList<ApplicationEntry> getResults() {
        return results;
    }

    public int getTotalHits(){
        return this.totalHits;
    }

    public PaginateInfo getPaginateInfo(){
        return this.paginateInfo;
    }
}
