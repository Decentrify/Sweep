package se.sics.ms.events;

import se.sics.kompics.Event;
import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PaginateInfo;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:16 PM
 */
public class UiSearchRequest implements KompicsEvent {

    private final SearchPattern pattern;
    private PaginateInfo paginateInfo;


    public UiSearchRequest(SearchPattern pattern) {
        this.pattern = pattern;
    }
    public UiSearchRequest(SearchPattern pattern, PaginateInfo paginateInfo) {

        this.pattern = pattern;
        this.paginateInfo = paginateInfo;
    }

    public SearchPattern getPattern() {
        return pattern;
    }

    public PaginateInfo getPaginateInfo(){
        return this.paginateInfo;
    }
}
