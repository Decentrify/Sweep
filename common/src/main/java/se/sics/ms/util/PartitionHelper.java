package se.sics.ms.util;


import org.javatuples.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;

import java.security.PublicKey;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/22/13
 * Time: 2:18 PM
 */
public class PartitionHelper {
    

    private static Logger logger = LoggerFactory.getLogger(PartitionHelper.class);

    /**
     * Returns next bit of the partitionId after partitioning is performed
     * @param yourNodeId node id
     * @param partitionId current partition id
     * @return next bit of the partition id
     */
    public static boolean determineYourNewPartitionSubId(int yourNodeId, PartitionId partitionId) {
        if(partitionId == null)
            throw new IllegalArgumentException("currentPartitionId can't be null");

        if(partitionId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE)
            return (yourNodeId & 1) != 0;

        return (yourNodeId & (1 << partitionId.getPartitionIdDepth())) != 0;
    }

    /**
     * Returns new partition id for SearchDescriptor
     * @param descriptor
     * @param isFirstPartition
     * @param bitsToCheck
     * @return
     */
    public static PartitionId determineSearchDescriptorPartition(SearchDescriptor descriptor, boolean isFirstPartition,
                                                                      int bitsToCheck) {
        if(descriptor == null)
            throw new IllegalArgumentException("descriptor can't be null");

        if(isFirstPartition)
            return new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE, 1, descriptor.getId() & 1);

        int partitionId = 0;

        for(int i=0; i<bitsToCheck; i++)
            partitionId = partitionId | (descriptor.getId() & (1<<i));

        return new PartitionId(VodAddress.PartitioningType.MANY_BEFORE, bitsToCheck, partitionId);
    }

    /**
     * Set's new new partitionId for address
     * @param searchDescriptor
     * @param partitionId
     * @return a copy of the updated VodAddress object
     */
    public static VodAddress updatePartitionId(SearchDescriptor searchDescriptor, PartitionId partitionId) {
        if(searchDescriptor.getVodAddress() == null)
            throw new IllegalArgumentException("address can't be null");
        if(partitionId == null)
            throw new IllegalArgumentException("partitionId can't be null");

        int categoryId = searchDescriptor.getOverlayId().getCategoryId();
        int newOverlayId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(partitionId.getPartitioningType(),
                partitionId.getPartitionIdDepth(), partitionId.getPartitionId(), categoryId);
        return new VodAddress(searchDescriptor.getVodAddress().getPeerAddress(), newOverlayId,
                searchDescriptor.getVodAddress().getNat(), searchDescriptor.getVodAddress().getParents());
    }

    /**
     * Ensures that all entries in descriptors belong to the same partition as self by recalculation of partitionIds
     * on this descriptors and throwing out those that are from another partitions
     * @param partitionId
     * @param descriptors
     */
    public static void adjustDescriptorsToNewPartitionId(PartitionId partitionId, Collection<SearchDescriptor> descriptors) {
        if(partitionId == null)
            throw new IllegalArgumentException("partitionId can't be null");
        if(descriptors == null)
            return;

        List<SearchDescriptor> updatedSample = new ArrayList<SearchDescriptor>();


        //this method has to be called after the partitionsNumber is already incremented
        boolean isFirstPartition = partitionId.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;
        if(isFirstPartition) {
            for(SearchDescriptor descriptor : descriptors) {
                PartitionId descriptorPartitionId = determineSearchDescriptorPartition(descriptor,
                        isFirstPartition, 1);

                VodAddress a = updatePartitionId(descriptor, descriptorPartitionId);
                updatedSample.add(new SearchDescriptor(a, descriptor));
            }
        }
        else {
            int bitsToCheck = partitionId.getPartitionIdDepth();

            for(SearchDescriptor descriptor : descriptors) {
                PartitionId descriptorPartitionId = determineSearchDescriptorPartition(descriptor,
                        isFirstPartition, bitsToCheck);

                VodAddress a = updatePartitionId(descriptor, descriptorPartitionId);
                updatedSample.add(new SearchDescriptor(a, descriptor));
            }
        }

        descriptors.clear();
        descriptors.addAll(updatedSample);

        Iterator<SearchDescriptor> iterator = descriptors.iterator();
        while (iterator.hasNext()) {
            OverlayAddress next = iterator.next().getOverlayAddress();
            if(next.getPartitionId() != partitionId.getPartitionId()
                    || next.getPartitionIdDepth() != partitionId.getPartitionIdDepth()
                    || next.getPartitioningType() != partitionId.getPartitioningType())
                iterator.remove();

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
    public static void updateBucketsInRoutingTable(PartitionId newPartitionId, Map<Integer,
            HashSet<SearchDescriptor>> categoryRoutingMap, HashSet<SearchDescriptor> bucket) {
        if(newPartitionId == null)
            throw new IllegalArgumentException("newPartitionId can't be null");

        if(newPartitionId.getPartitionId() == 0)
            return;

        //if first split
        if(newPartitionId.getPartitionIdDepth() == 1) {
            HashSet<SearchDescriptor> oldBucket = categoryRoutingMap.get(0);
            if(oldBucket == null)
                return;

            Iterator<SearchDescriptor> oldBucketIterator = oldBucket.iterator();
            while(oldBucketIterator.hasNext()) {
                SearchDescriptor next = oldBucketIterator.next();
                OverlayAddress nextAddress = next.getOverlayAddress();
                boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(next.getId(),
                        new PartitionId(nextAddress.getPartitioningType(), nextAddress.getPartitionIdDepth(),
                                nextAddress.getPartitionId()));
                //first spilling => move to a new bucket all with "true"
                if(partitionSubId) {
                    PartitionId descriptorsPartitionId = new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE,
                            1, 1);
                    VodAddress a = updatePartitionId(next, descriptorsPartitionId);
                    oldBucketIterator.remove();
                    bucket.add(new SearchDescriptor(a, next));
                }
            }

            return;
        }

        int oldPartitionId = getPreviousPartitionId(newPartitionId);
        HashSet<SearchDescriptor> oldBucket = categoryRoutingMap.get(oldPartitionId);
        if(oldBucket == null)
            return;

        Iterator<SearchDescriptor> oldBucketIterator = oldBucket.iterator();
        while(oldBucketIterator.hasNext()) {
            SearchDescriptor next = oldBucketIterator.next();
            OverlayAddress nextAddress = next.getOverlayAddress();
            boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(next.getId(),
                    new PartitionId(nextAddress.getPartitioningType(), nextAddress.getPartitionIdDepth(),
                            nextAddress.getPartitionId()));

            VodAddress a = updatePartitionId(next, new PartitionId(VodAddress.PartitioningType.MANY_BEFORE,
                    nextAddress.getPartitionIdDepth() + 1,
                    partitionSubId ? nextAddress.getPartitionId() | (1 << nextAddress.getPartitionIdDepth()) :
                    nextAddress.getPartitionIdDepth()));


            boolean isOne = ((nextAddress.getPartitionId() & (1 << nextAddress.getPartitionIdDepth()-1)) == 1);
            //move to a new bucket all with first "true"
            if(isOne) {
                oldBucketIterator.remove();
                bucket.add(new SearchDescriptor(a, next));
            }
        }

    }

    /**
     * Helper method to spill the contents in new bucket as a new partition is detected.
     * Based on the new partition id passed, look at the previous buckets and then remove the contents.
     * Create new buckets for left spill and right spill. <br\><br\>
     *
     * <b>CAUTION:</b> For now given that partition is an event which would happen after a long time, we will be only looking at the bucket before us.
     * If the bucket is present then move the contents to new buckets respectively. For the case in which the difference between the buckets
     * is greater than 1, needs to be handled separpartitionIdately.
     *
     * @param partition Updated Partition Id.
     * @param categoryRoutingMap Current Routing Map.
     */
    public static void removeOldBuckets(PartitionId partition, Map<Integer, Pair<Integer, HashMap<VodAddress, CroupierPeerView>>> categoryRoutingMap){


        if( partition== null ){
            throw new IllegalArgumentException("Partition Id is null");
        }

        if(partition.getPartitionIdDepth() == 0)
            return;

        int oldPartitionId = getPreviousPartitionId(partition);

        if(categoryRoutingMap.get(oldPartitionId) == null){
            logger.warn("Unable to find partition bucket for previous partition: {}", oldPartitionId);
            return;
        }

        // Simply remove the old map. We can remove here the old map because the method is called when we couldn't find any map with the id passed.
        categoryRoutingMap.remove(oldPartitionId);
    }

    /**
     * Based on the partitionId value provided, you actually flip the at the position given by the partition depth.
     * Flipping the bit will give the partition Id of the other half at that level.
     *
     * @param partitionId
     * @return
     */
    public static int getPartitionIdOtherHalf(PartitionId partitionId) {

        int requiredPartitionId = 0;

        if (partitionId.getPartitionIdDepth() == 0) {
            return requiredPartitionId;
        }

        // Simply flip the bit of the partition at the required position.
        requiredPartitionId = partitionId.getPartitionId() ^ ( 1 <<  (partitionId.getPartitionIdDepth() - 1 ));
        return requiredPartitionId;
    }

    public static int getPreviousPartitionId(PartitionId partitionId) {

        int mask = 0;

        for(int i = 0; i < partitionId.getPartitionIdDepth() - 1; i++) {

            mask|=(1<<i);
        }

        return partitionId.getPartitionId() & mask;

    }

    /**
     * Partition Information stored in the class.
     */
    public static class PartitionInfo{

        private long medianId;
        private TimeoutId requestId;
        private VodAddress.PartitioningType partitioningType;
        private String hash;
        private PublicKey key;

        public PartitionInfo(long medianId, TimeoutId requestId, VodAddress.PartitioningType partitioningType){
            this.medianId = medianId;
            this.partitioningType = partitioningType;
            this.requestId = requestId;
        }


        public PartitionInfo(long medianId, TimeoutId requestId, VodAddress.PartitioningType partitioningType, String hash, PublicKey key){
            this(medianId,requestId,partitioningType);
            this.hash = hash;
            this.key = key;
        }

        public long getMedianId(){
            return this.medianId;
        }

        public VodAddress.PartitioningType getPartitioningTypeInfo(){
            return this.partitioningType;
        }

        public TimeoutId getRequestId(){
            return this.requestId;
        }

        /**
         * Hash of the Partition Information.
         * @return
         */
        public String getHash(){
            return this.hash;
        }

        /**
         * Returns the public key of the leader that initiated the partition.
         * @return
         */
        public PublicKey getKey(){
            return this.key;
        }


        /**
         * Set the hash value for the object.
         * @param hash
         */
        public void setHash(String hash){
            this.hash = hash;
        }

        /**
         * Sets the public key for the object.
         * @param key
         */
        public void setKey(PublicKey key){
            this.key = key;
        }

        // TODO: Missing entry for the equals and #-code method.

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof PartitionInfo){
                PartitionInfo other = (PartitionInfo)obj;
                if(other.requestId.equals(requestId))
                    return true;
            }
            return false;
        }

        // FIXME: Correct the hashCode generation mechanism.
        @Override
        public int hashCode() {
            return requestId.hashCode();
        }
    }


    /**
     * Contains the hash information regarding the partitioning update.
     * @author babbarshaer
     */
    public static class PartitionInfoHash {

        TimeoutId partitionRequestId;
        String hash;

        public PartitionInfoHash (TimeoutId partitionRequestId, String hash){
            this.partitionRequestId = partitionRequestId;
            this.hash = hash;
        }

        /**
         * Convenience Constructor.
         * @param partitionInfo
         */
        public PartitionInfoHash(PartitionInfo partitionInfo){

            this.partitionRequestId = partitionInfo.getRequestId();
            this.hash = partitionInfo.getHash();
        }


        public TimeoutId getPartitionRequestId(){
            return this.partitionRequestId;
        }

        public String getHash(){
            return this.hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionInfoHash that = (PartitionInfoHash) o;

            if (!hash.equals(that.hash)) return false;
            if (!partitionRequestId.equals(that.partitionRequestId)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionRequestId.hashCode();
            result = 31 * result + hash.hashCode();
            return result;
        }
    }

    
}
