package se.sics.ms.gradient.gradient;

import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.gradient.api.GradientFilter;

/**
 * Application's Gradient Filter.
 *
 * Created by babbarshaer on 2015-03-06.
 */
public class SweepGradientFilter implements GradientFilter<SearchDescriptor>{
    
    @Override
    public boolean retainOther(SearchDescriptor selfDescriptor, SearchDescriptor otherDescriptor) {
        return false;
    }

    @Override
    public boolean cleanOldView(SearchDescriptor newDescriptor, SearchDescriptor oldDescriptor) {
        return false;
    }
}
