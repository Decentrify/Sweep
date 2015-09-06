
package se.sics.ms.aggregator;

import se.sics.ktoolbox.aggregator.global.api.system.ComponentInfo;
import se.sics.ms.types.PeerDescriptor;

/**
 * Information contained in the search component.
 * This is supposed to be a big blob of information from the component.
 *
 * Created by babbar on 2015-09-06.
 */
public class SearchComponentInfo implements ComponentInfo{

    private PeerDescriptor descriptor;
    private float searchResponse;

    public SearchComponentInfo (PeerDescriptor descriptor, float searchResponse){

        this.descriptor = descriptor;
        this.searchResponse = searchResponse;
    }

    @Override
    public String toString() {
        return "SearchComponentInfo{" +
                "descriptor=" + descriptor +
                ", searchResponse=" + searchResponse +
                '}';
    }

    public PeerDescriptor getDescriptor() {
        return descriptor;
    }

    public float getSearchResponse() {
        return searchResponse;
    }
}
