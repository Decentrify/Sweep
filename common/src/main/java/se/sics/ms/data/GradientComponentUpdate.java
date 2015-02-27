package se.sics.ms.data;

import se.sics.ms.types.SearchDescriptor;

/**
 * Created by babbarshaer on 2015-02-19.
 */
public class GradientComponentUpdate implements ComponentUpdate {

    public static String GRADIENT_KEY = "gradient";

    private final SearchDescriptor searchDesc;

    public GradientComponentUpdate(SearchDescriptor desc) {
        this.searchDesc = desc;
    }

    public SearchDescriptor getSearchDescriptor(){
        return this.searchDesc;
    }

}