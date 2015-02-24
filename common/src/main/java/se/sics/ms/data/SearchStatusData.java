package se.sics.ms.data;

import se.sics.ms.types.SearchDescriptor;

/**
 * 
 * Represents data from the
 * Created by babbarshaer on 2015-02-19.
 */
public class SearchStatusData implements ComponentStatus {
    
    public static String SEARCH_KEY = "search";

    private final SearchDescriptor searchDesc;

    public SearchStatusData(SearchDescriptor desc) {
        this.searchDesc = desc;
    }

    public SearchDescriptor getSearchDescriptor(){
        return this.searchDesc;
    }
    
}
