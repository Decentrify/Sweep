package se.sics.ms.events;

import se.sics.kompics.Event;
import se.sics.ms.types.SearchPattern;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:16 PM
 */
public class UiSearchRequest extends Event {
    private final SearchPattern pattern;

    public UiSearchRequest(SearchPattern pattern) {
        this.pattern = pattern;
    }

    public SearchPattern getPattern() {
        return pattern;
    }
}
