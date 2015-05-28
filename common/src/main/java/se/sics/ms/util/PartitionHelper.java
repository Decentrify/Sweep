package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.OverlayId;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.SearchDescriptor;

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
     * Check if the received overlay is a logical extension of the base self overlay
     * id information.
     *
     * @param receivedId receivedId
     * @param selfId selfId
     * @param nodeId nodeId
     * @return Extension
     */
    public static boolean isOverlayExtension(int receivedId, int selfId, int nodeId){

        boolean result = false;

        if(selfId == receivedId){
            result = true;
        }

        OverlayId selfOverlay = new OverlayId(selfId);
        OverlayId receivedOverlay = new OverlayId(receivedId);

        if(receivedOverlay.getPartitionIdDepth() > selfOverlay.getPartitionIdDepth()){

            int bitsToCheck = receivedOverlay.getPartitionIdDepth();
            int partitionId = 0;

            for(int i=0; i<bitsToCheck; i++)
                partitionId = partitionId | (nodeId & (1<<i));

            if(partitionId == receivedOverlay.getPartitionId())
                result = true;
        }

        return result;



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
        private UUID requestId;
        private VodAddress.PartitioningType partitioningType;
        private String hash;
        private PublicKey key;

        public PartitionInfo(long medianId, UUID requestId, VodAddress.PartitioningType partitioningType){
            this.medianId = medianId;
            this.partitioningType = partitioningType;
            this.requestId = requestId;
        }


        public PartitionInfo(long medianId, UUID requestId, VodAddress.PartitioningType partitioningType, String hash, PublicKey key){
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

        public UUID getRequestId(){
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

        UUID partitionRequestId;
        String hash;

        public PartitionInfoHash (UUID partitionRequestId, String hash){
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


        public UUID getPartitionRequestId(){
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
