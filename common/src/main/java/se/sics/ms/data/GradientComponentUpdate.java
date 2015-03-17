package se.sics.ms.data;

import se.sics.ms.types.SearchDescriptor;

/**
 * Update from the gradient component.
 * 
 * Created by babbarshaer on 2015-02-19.
 */
public class GradientComponentUpdate implements ComponentUpdate {

    private final SearchDescriptor searchDesc;

    public GradientComponentUpdate(SearchDescriptor desc) {
        this.searchDesc = desc;
    }

    public SearchDescriptor getSearchDescriptor(){
        return this.searchDesc;
    }

}