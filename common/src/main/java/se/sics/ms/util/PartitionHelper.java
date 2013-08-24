package se.sics.ms.util;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.common.MsSelfImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/22/13
 * Time: 2:18 PM
 */
public class PartitionHelper {
    /**
     * Returns next bit of the partitionId after partitioning is performed
     * @param yourNodeId node id
     * @param currentPartitionId current partition id
     * @param isFirstPartition true if it's after first partitioning
     * @return next bit of the partition id
     */
    public static boolean determineYourPartition(int yourNodeId, LinkedList<Boolean> currentPartitionId,
                                                 boolean isFirstPartition) {
        if(currentPartitionId == null)
            throw new IllegalArgumentException("currentPartitionId can't be null");

        if(isFirstPartition) {
            boolean partitionSubId = (yourNodeId & 1) != 0;

            LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
            partitionId.addFirst(partitionSubId);

            return partitionSubId;
        }

        LinkedList partitionId = currentPartitionId;
        int partitionIdLength = partitionId.size();

        boolean partitionSubId = (yourNodeId & (1 << partitionIdLength)) != 0;
        partitionId.addFirst(partitionSubId);

        return partitionSubId;
    }

    /**
     * Returns new partition id for VodDescriptor
     * @param descriptor
     * @param isFirstPartition
     * @param bitsToCheck
     * @return
     */
    public static LinkedList<Boolean> determineVodDescriptorPartition(VodDescriptor descriptor, boolean isFirstPartition,
                                                                      int bitsToCheck) {
        if(descriptor == null)
            throw new IllegalArgumentException("descriptor can't be null");

        if(isFirstPartition) {
            LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
            partitionId.addFirst((descriptor.getId() & 1) != 0);
            return partitionId;
        }

        LinkedList<Boolean> partitionId = new LinkedList<Boolean>();

        for(int i=0; i<bitsToCheck; i++) {
            partitionId.addFirst((descriptor.getId() & (1<<i)) != 0);
        }
        return partitionId;
    }

    public static void adjustDescriptorsToNewPartitionId(MsSelfImpl self, Collection<VodDescriptor> descriptors) {
        if(descriptors == null)
            return;

        //this method has to be called after the partitionsNumber is already incremented
        boolean isFirstPartition = self.getPartitionsNumber() == 2;
        if(isFirstPartition) {
            for(VodDescriptor descriptor : descriptors) {
                LinkedList<Boolean> partitionId = determineVodDescriptorPartition(descriptor,
                        isFirstPartition, 1);

                descriptor.setPartitionId(partitionId);
                descriptor.setPartitionsNumber(self.getPartitionsNumber());
            }
        }
        else {
            int bitsToCheck = self.getPartitionId().size();

            for(VodDescriptor descriptor : descriptors) {
                LinkedList<Boolean> partitionId = determineVodDescriptorPartition(descriptor,
                        isFirstPartition, bitsToCheck);

                descriptor.setPartitionId(partitionId);
                descriptor.setPartitionsNumber(self.getPartitionsNumber());
            }
        }

        Iterator<VodDescriptor> iterator = descriptors.iterator();
        while (iterator.hasNext()) {
            VodDescriptor next = iterator.next();
            if(!next.getPartitionId().equals(self.getPartitionId()))  {
                iterator.remove();
            }
        }
    }

    public static int LinkedListPartitionToInt(LinkedList<Boolean> partition) {
        int partitionId = 0;
        int j = 0;
        for(int i = partition.size()-1; i >= 0; i--) {
            if(partition.get(i))
                partitionId = partitionId | (1 << j++);
        }

        return partitionId;
    }
}
