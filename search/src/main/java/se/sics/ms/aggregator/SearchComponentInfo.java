
package se.sics.ms.aggregator;

import se.sics.ktoolbox.aggregator.global.api.system.ComponentInfo;
import se.sics.ms.types.PeerDescriptor;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Information contained in the search component.
 * This is supposed to be a big blob of information from the component.
 *
 * Created by babbar on 2015-09-06.
 */
public class SearchComponentInfo implements ComponentInfo{

    private PeerDescriptor descriptor;
    private float searchResponse;
    private DecoratedAddress leaderInfo;

    public SearchComponentInfo (PeerDescriptor descriptor, float searchResponse, DecoratedAddress leaderInfo){

        this.descriptor = descriptor;
        this.searchResponse = searchResponse;
        this.leaderInfo = leaderInfo;
    }

    @Override
    public String toString() {
        return "SearchComponentInfo{" +
                "descriptor=" + descriptor +
                ", searchResponse=" + searchResponse +
                ", leaderInfo=" + leaderInfo +
                '}';
    }

    public PeerDescriptor getDescriptor() {
        return descriptor;
    }

    public float getSearchResponse() {
        return searchResponse;
    }

    public DecoratedAddress getLeaderInfo() {
        return leaderInfo;
    }
}
