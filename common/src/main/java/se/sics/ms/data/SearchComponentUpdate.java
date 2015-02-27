package se.sics.ms.data;

import se.sics.ms.types.SearchDescriptor;

/**
 * 
 * Represents data from the
 * Created by babbarshaer on 2015-02-19.
 */
public class SearchComponentUpdate implements ComponentUpdate {
    
    public static String SEARCH_KEY = "search";

    private final SearchDescriptor searchDesc;

    public SearchComponentUpdate(SearchDescriptor desc) {
        this.searchDesc = desc;
    }

    public SearchDescriptor getSearchDescriptor(){
        return this.searchDesc;
    }
    
}
