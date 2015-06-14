package se.sics.ms.gradient.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.SearchDescriptor;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.gradient.GradientFilter;

/**
 * Application's Gradient Filter.
 *
 * Created by babbarshaer on 2015-03-06.
 */
public class SweepGradientFilter implements GradientFilter<SearchDescriptor> {
    
    @Override
    public boolean retainOther(SearchDescriptor selfDescriptor, SearchDescriptor otherDescriptor) {

        OverlayAddress selfOverlayAddress = selfDescriptor.getOverlayAddress();
        OverlayAddress otherOverlayAddress = otherDescriptor.getOverlayAddress();

        // Donot allow other category nodes in the system.
        if(selfOverlayAddress.getCategoryId() != otherOverlayAddress.getCategoryId()){
            return false;
        }

        if(selfOverlayAddress.getPartitionIdDepth() > otherOverlayAddress.getPartitionIdDepth()){
            return false;
        }

        int selfOverlayId = selfOverlayAddress
                .getOverlayId()
                .getId();

        int receivedOverlayId = otherOverlayAddress
                .getOverlayId()
                .getId();


        return PartitionHelper.isOverlayExtension(receivedOverlayId, selfOverlayId, otherDescriptor.getId());

    }

    @Override
    public boolean cleanOldView(SearchDescriptor oldDescriptor, SearchDescriptor newDescriptor) {
        return newDescriptor.getOverlayAddress().getPartitionIdDepth() > oldDescriptor.getOverlayAddress().getPartitionIdDepth();
    }
}
