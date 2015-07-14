package se.sics.ms.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.ApplicationEntry;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 9:56 AM
 */
public class UiSearchResponse implements KompicsEvent {
    
    private ArrayList<ApplicationEntry> results;
    public UiSearchResponse(ArrayList<ApplicationEntry> result) {
        this.results = result;
    }
    public ArrayList<ApplicationEntry> getResults() {
        return results;
    }
}
