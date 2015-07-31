package se.sics.ms.gradient.gradient;

import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.PartitionHelper;
import se.sics.ms.util.PartitioningType;
import se.sics.p2ptoolbox.gradient.GradientFilter;

/**
 * Application's Gradient Filter.
 *
 * Created by babbarshaer on 2015-03-06.
 */
public class SweepGradientFilterOld implements GradientFilter<PeerDescriptor> {
    
    @Override
    public boolean retainOther(PeerDescriptor selfDescriptor, PeerDescriptor otherDescriptor) {

        OverlayAddress selfOverlayAddress = selfDescriptor.getOverlayAddress();
        OverlayAddress otherOverlayAddress = otherDescriptor.getOverlayAddress();

        if(selfOverlayAddress.getPartitionIdDepth() > otherOverlayAddress.getPartitionIdDepth()){
            return false;
        }
        
        else if (selfOverlayAddress.getPartitionIdDepth() < otherOverlayAddress.getPartitionIdDepth()){
            
            boolean isNeverBefore = selfOverlayAddress.getPartitioningType() == PartitioningType.NEVER_BEFORE;

            if(!isNeverBefore){

                int bitsToCheck = selfOverlayAddress.getPartitionIdDepth();
                boolean isOnceBefore = selfOverlayAddress.getPartitioningType() == PartitioningType.ONCE_BEFORE;
                
                PartitionId generatedPartitionId = PartitionHelper.determineSearchDescriptorPartition(
                        otherDescriptor,
                        isOnceBefore,
                        bitsToCheck);

                return (generatedPartitionId.getPartitionId() == selfOverlayAddress.getPartitionId());
            }
            
            return true;
        }
        
        return ( !(otherOverlayAddress.getCategoryId() != selfOverlayAddress.getCategoryId()
                || otherOverlayAddress.getPartitionId() != selfOverlayAddress.getPartitionId()) );
    }

    @Override
    public boolean cleanOldView(PeerDescriptor oldDescriptor, PeerDescriptor newDescriptor) {
        return newDescriptor.getOverlayAddress().getPartitionIdDepth() > oldDescriptor.getOverlayAddress().getPartitionIdDepth();
    }
}
