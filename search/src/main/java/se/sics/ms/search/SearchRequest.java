package se.sics.ms.search;

import se.sics.kompics.Event;
import se.sics.peersearch.types.SearchPattern;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:16 PM
 */
public class SearchRequest extends Event {
    private final SearchPattern pattern;

    public SearchRequest(SearchPattern pattern) {
        this.pattern = pattern;
    }

    public SearchPattern getPattern() {
        return pattern;
    }
}
