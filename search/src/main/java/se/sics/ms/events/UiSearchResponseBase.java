package se.sics.ms.events;

import se.sics.kompics.Event;
import se.sics.ms.types.IndexEntry;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 9:56 AM
 */
public class UiSearchResponseBase extends Event {
    private ArrayList<IndexEntry> results;

    public UiSearchResponseBase(ArrayList<IndexEntry> result) {
        this.results = result;
    }

    public ArrayList<IndexEntry> getResults() {
        return results;
    }
}
