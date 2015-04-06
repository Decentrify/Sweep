package se.sics.ms.types;

import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.io.Serializable;

/**
 * Main descriptor used by application to indicate the current state of the 
 * node.
 *
 * Created by alidar on 8/11/14.
 */
public class SearchDescriptor implements DescriptorBase, Comparable<SearchDescriptor>, Serializable, PeerView, LCPeerView{

//    private int age;
    private transient boolean connected;
    private OverlayAddress overlayAddress;
    private final long numberOfIndexEntries;
    private final boolean isLGMember;

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress) {
        this(new OverlayAddress(vodAddress), false, 0, false);
    }

    public SearchDescriptor(SearchDescriptor descriptor){
        this(descriptor.getOverlayAddress(), descriptor.isConnected(), descriptor.getNumberOfIndexEntries(), descriptor.isLGMember());
    }

    public SearchDescriptor(OverlayAddress overlayAddress, boolean connected, long numberOfIndexEntries, boolean isLGMember){
        this.overlayAddress = overlayAddress;
        this.connected = connected;
        this.numberOfIndexEntries = numberOfIndexEntries;
        this.isLGMember = isLGMember;
    }

    public boolean isLGMember() {
        return isLGMember;
    }

    public VodAddress getVodAddress() {
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
        return (this.overlayAddress.toString() + " entries: " + this.getNumberOfIndexEntries() + " member: " + this.isLGMember);
    }

    public OverlayAddress getOverlayAddress() {
        return overlayAddress;
    }


    @Override
    public int compareTo(SearchDescriptor that) {

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

        int indexEntryCompareResult = Long.valueOf(this.numberOfIndexEntries).compareTo(Long.valueOf(that.numberOfIndexEntries));

        if(indexEntryCompareResult != 0){
            return indexEntryCompareResult;
        }

        // NOTE: Fix this because internally it uses the VodAddress compareTo , which uses overlay Id as the tie breaker.
        return this.overlayAddress.getAddress().compareTo(that.overlayAddress.getAddress());
    }

    @Override
    public int hashCode() {
        final int prime = 87;
        int result = 1;
        result = prime * result + ((this.getOverlayAddress() == null) ? 0 : this.getOverlayAddress().hashCode());
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
        SearchDescriptor other = (SearchDescriptor) obj;

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

        return (this.isLGMember == other.isLGMember);
    }

    public long getNumberOfIndexEntries() {
        return numberOfIndexEntries;
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
    
    @Override
    public SearchDescriptor deepCopy() {
        return new SearchDescriptor(this.getOverlayAddress(), this.isConnected(), this.getNumberOfIndexEntries(), this.isLGMember());
    }

    @Override
    public LCPeerView enableLGMembership() {
        return new SearchDescriptor(this.overlayAddress, this.connected, this.numberOfIndexEntries, true);
    }

    @Override
    public LCPeerView disableLGMembership() {
        return new SearchDescriptor(this.overlayAddress, this.connected, this.numberOfIndexEntries, false);
    }
}