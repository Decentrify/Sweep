package se.sics.ms.types;

import java.io.Serializable;
import se.sics.ktoolbox.election.util.LCPeerView;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.update.view.View;

/**
 * Main descriptor used by application to indicate the current state of the 
 * node.
 *
 * Created by alidar on 8/11/14.
 */
public class PeerDescriptor implements DescriptorBase, Comparable<PeerDescriptor>, Serializable, LCPeerView, View {

//    private int age;
    private transient boolean connected;
    private OverlayAddress overlayAddress;
    private final long numberOfIndexEntries;
    private final boolean isLGMember;
    private LeaderUnit lastLeaderUnit;

    public PeerDescriptor(KAddress address) {
        this(new OverlayAddress(address, 0), false, 0, false, null);
    }

    public PeerDescriptor(KAddress address, int overlayId){
        this(new OverlayAddress(address, overlayId), false, 0, false, null);
    }

    public PeerDescriptor(PeerDescriptor descriptor){
        this(new OverlayAddress(descriptor.getOverlayAddress().getAddress(), descriptor.getOverlayId().getId()), descriptor.isConnected(), descriptor.getNumberOfIndexEntries(), descriptor.isLeaderGroupMember(), descriptor.getLastLeaderUnit());
    }

    public PeerDescriptor(OverlayAddress overlayAddress, boolean connected, long numberOfIndexEntries, boolean isLGMember, LeaderUnit lastLeaderUnit){
        this.overlayAddress = overlayAddress;
        this.connected = connected;
        this.numberOfIndexEntries = numberOfIndexEntries;
        this.isLGMember = isLGMember;
        this.lastLeaderUnit = lastLeaderUnit;
    }


    public KAddress getVodAddress() {
        return this.overlayAddress.getAddress();
    }

    public int getId() {
        return this.overlayAddress.getId();
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public OverlayId getOverlayId() { return this.overlayAddress.getOverlayId();}

    @Override
    public String toString() {
        return ("PeerDescriptor :{ " + this.overlayAddress.toString() + " entries: " + this.getNumberOfIndexEntries() + " member: " + this.isLGMember + " }");
    }

    public OverlayAddress getOverlayAddress() {
        return overlayAddress;
    }

    public int compareTo(PeerDescriptor that) {

        if(that == null){
            throw new RuntimeException("Can't compare to a null descriptor.");
        }

        if(equals(that)){
            return 0;
        }

        if(this.getOverlayAddress() == null || that.getOverlayAddress() == null){
            throw new IllegalArgumentException(" Parameters to compare are null");
        }

        if(this.isLGMember != that.isLGMember){

            if(this.isLGMember){
                return 1;
            }
            else{
                return -1;
            }
        }


        int overlayIdComparisonResult = this.overlayAddress.getOverlayId().compareTo(that.overlayAddress.getOverlayId());

        if(overlayIdComparisonResult != 0){
            return overlayIdComparisonResult;
        }

        int indexEntryCompareResult = Long.valueOf(this.numberOfIndexEntries).compareTo(that.numberOfIndexEntries);

        if(indexEntryCompareResult != 0){
            return indexEntryCompareResult;
        }

        // NOTE: Fix this because internally it uses the VodAddress compareTo , which uses overlay Id as the tie breaker.
        return this.overlayAddress.compareTo(that.overlayAddress);
    }

    @Override
    public int hashCode() {
        int result = (connected ? 1 : 0);
        result = 31 * result + (overlayAddress != null ? overlayAddress.hashCode() : 0);
        result = 31 * result + (int) (numberOfIndexEntries ^ (numberOfIndexEntries >>> 32));
        result = 31 * result + (isLGMember ? 1 : 0);
        result = 31 * result + (lastLeaderUnit != null ? lastLeaderUnit.hashCode() : 0);
        return result;
    }

    /**
     *  In order to keep the contract synchronized and include all the fields, the equality
     *  check also incorporates the index entries in system.
     *
     *  As index entries are dynamic i.e keep changing with each iteration, this check will prevent
     *  the detection of the old entries with same address but different entry strength.
     *
     *  Care needs to be taken in terms of explicitly iterating over the sample set to remove the duplicates in terms
     *  of nodes with similar address.
     *
     * @param obj Other Object to compare to.
     * @return true/false in case entry is equal or not.
     */
    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }
        PeerDescriptor other = (PeerDescriptor) obj;

        if (this.overlayAddress == null) {
            if (other.getOverlayAddress() != null) {
                return false;
            }
        } else if (other.overlayAddress == null) {
            return false;
        }

        if (!(this.overlayAddress.equals(other.overlayAddress))) {
            return false;
        }


        else if(!(this.numberOfIndexEntries == other.numberOfIndexEntries)){
            return false;
        }

        else if (this.isLGMember == other.isLGMember);
        
        return (this.lastLeaderUnit == null 
                ? other.lastLeaderUnit == null 
                : (other.lastLeaderUnit != null && this.lastLeaderUnit.equals(other.lastLeaderUnit)));
    }

    public long getNumberOfIndexEntries() {
        return numberOfIndexEntries;
    }

    public LeaderUnit getLastLeaderUnit() {
        return lastLeaderUnit;
    }

    /**
     * Wrapper over the access of partitioning depth from the
     * overlay address.
     *
     * @return originalPartitioning Depth.
     */
    public int getPartitioningDepth(){
        return this.overlayAddress.getPartitionIdDepth();
    }
    
    public boolean isLeaderGroupMember() {
        return this.isLGMember;
    }

    public LCPeerView enableLGMembership() {
        return new PeerDescriptor(this.overlayAddress, this.connected, this.numberOfIndexEntries, true, this.lastLeaderUnit);
    }

    public LCPeerView disableLGMembership() {
        return new PeerDescriptor(this.overlayAddress, this.connected, this.numberOfIndexEntries, false, this.lastLeaderUnit);
    }
}