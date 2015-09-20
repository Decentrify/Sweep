package se.sics.ms.aggregator.processor;

import se.sics.ktoolbox.aggregator.client.util.ComponentInfoProcessor;
import se.sics.ms.aggregator.SearchComponentInfo;
import se.sics.ms.data.InternalStatePacket;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PeerDescriptor;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Processor at the local aggregator level for creation of the 
 * packet information which will be shipped to the global aggregator.
 * 
 * Created by babbarshaer on 2015-09-09.
 */
public class CompInternalStateProcessor implements ComponentInfoProcessor<SearchComponentInfo, InternalStatePacket> {
    
    
    public InternalStatePacket processComponentInfo(SearchComponentInfo searchComponentInfo) {

        PeerDescriptor descriptor = searchComponentInfo.getDescriptor();
        OverlayAddress overlay = descriptor.getOverlayAddress();

        DecoratedAddress leaderAddress = searchComponentInfo.getLeaderInfo();
        Integer leaderId = leaderAddress != null ? leaderAddress.getId() : null;

        return new InternalStatePacket(descriptor.getId(), overlay.getPartitionId(), overlay.getPartitionIdDepth(),
                leaderId, descriptor.getNumberOfIndexEntries());
    }
}
