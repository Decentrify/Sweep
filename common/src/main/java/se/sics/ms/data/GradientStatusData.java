package se.sics.ms.data;

import se.sics.ms.types.SearchDescriptor;

/**
 * Created by babbarshaer on 2015-02-19.
 */
public class GradientStatusData implements ComponentStatus {

    public static String GRADIENT_KEY = "gradient";

    private final SearchDescriptor searchDesc;

    public GradientStatusData(SearchDescriptor desc) {
        this.searchDesc = desc;
    }

    public SearchDescriptor getSearchDescriptor(){
        return this.searchDesc;
    }

}