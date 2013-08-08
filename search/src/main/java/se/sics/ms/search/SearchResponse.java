package se.sics.ms.search;

import se.sics.kompics.Event;
import se.sics.peersearch.types.IndexEntry;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 9:56 AM
 */
public class SearchResponse extends Event {
    private ArrayList<IndexEntry> results;

    public SearchResponse(ArrayList<IndexEntry> result) {
        this.results = result;
    }

    public ArrayList<IndexEntry> getResults() {
        return results;
    }
}
