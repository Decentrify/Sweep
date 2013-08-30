package se.sics.ms.util;

import se.sics.gvod.common.VodDescriptor;
import se.sics.ms.common.MsSelfImpl;

import java.util.*;

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
    public static boolean determineYourNewPartitionSubId(int yourNodeId, LinkedList<Boolean> currentPartitionId,
                                                         boolean isFirstPartition) {
        if(currentPartitionId == null)
            throw new IllegalArgumentException("currentPartitionId can't be null");

        if(isFirstPartition)
            return (yourNodeId & 1) != 0;

        return (yourNodeId & (1 << currentPartitionId.size())) != 0;
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

    /**
     * Ensures that all entries in descriptors belong to the same partition as self by recalculation of partitionIds
     * on this descriptors and throwing out those that are from another partitions
     * @param self
     * @param descriptors
     */
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

    /**
     * Converts a partitionId as LinkedList<Boolean> to Integer
     * @param partition partitionId as LinkedList<Boolean>
     * @return partitionId as integer
     */
    public static int LinkedListPartitionToInt(LinkedList<Boolean> partition) {
        int partitionId = 0;
        int j = 0;
        for(int i = partition.size()-1; i >= 0; i--) {
            if(partition.get(i))
                partitionId = partitionId | (1 << j++);
        }

        return partitionId;
    }

    /**
     * updates a bucket in the categoryRoutingMap regarding new partitionId
     * @param newPartitionId unseen before partition id
     * @param categoryRoutingMap map for looking for an old bucket
     * @param bucket bucket for new partition id
     */
    public static void updateBucketsInRoutingTable(LinkedList<Boolean> newPartitionId, Map<Integer,
            HashSet<VodDescriptor>> categoryRoutingMap, HashSet<VodDescriptor> bucket) {
        if(newPartitionId.isEmpty() || LinkedListPartitionToInt(newPartitionId) == 0)
            return;

        //if first split
        if(newPartitionId.size() == 1) {
            HashSet<VodDescriptor> oldBucket = categoryRoutingMap.get(0);
            if(oldBucket == null)
                return;

            Iterator<VodDescriptor> oldBucketIterator = oldBucket.iterator();
            while(oldBucketIterator.hasNext()) {
                VodDescriptor next = oldBucketIterator.next();
                boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(next.getId(),
                        next.getPartitionId(), true);
                //first spilling => move to a new bucket all with "true"
                if(partitionSubId) {
                    LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
                    partitionId.addFirst(true);
                    next.setPartitionId(partitionId);
                    oldBucketIterator.remove();
                    bucket.add(next);
                }
            }

            return;
        }

        LinkedList<Boolean> partitionIdCopy = (LinkedList<Boolean>)newPartitionId.clone();
        partitionIdCopy.removeFirst();
        int oldPartitionId = PartitionHelper.LinkedListPartitionToInt(partitionIdCopy);
        HashSet<VodDescriptor> oldBucket = categoryRoutingMap.get(oldPartitionId);
        if(oldBucket == null)
            return;

        Iterator<VodDescriptor> oldBucketIterator = oldBucket.iterator();
        while(oldBucketIterator.hasNext()) {
            VodDescriptor next = oldBucketIterator.next();
            boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(next.getId(),
                    next.getPartitionId(), false);
            next.getPartitionId().addFirst(partitionSubId);

            //move to a new bucket all with first "true"
            if(partitionSubId == newPartitionId.getFirst()) {
                oldBucketIterator.remove();
                bucket.add(next);
            }
        }

    }
}
