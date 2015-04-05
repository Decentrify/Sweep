package se.sics.ms.types;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Descriptor used by main application to transmit information about self in the system.
 *
 * @author babbar
 */
public class SearchDescriptorCopy implements DescriptorBase, Comparable<SearchDescriptorCopy>, Serializable, PeerView {

    private transient boolean connected;
    private OverlayAddress overlayAddress;
    private final long numberOfIndexEntries;
    private boolean isLeaderGroupMember;
    
    //// Conversion functions
    // FIXME: Do we still need VodDescriptor. If not Fix it. ?
    
    public static VodDescriptor toVodDescriptor(SearchDescriptorCopy searchDescriptor) {
        VodDescriptor descriptor = new VodDescriptor(searchDescriptor.getVodAddress(), searchDescriptor.getNumberOfIndexEntries());
        descriptor.setConnected(searchDescriptor.isConnected());

        return descriptor;
    }

    public static List<VodDescriptor> toVodDescriptorList(List<SearchDescriptorCopy> searchDescriptors)
    {
        ArrayList<VodDescriptor> descriptorList = new ArrayList<VodDescriptor>();

        for(SearchDescriptorCopy descriptor: searchDescriptors) {
            descriptorList.add(SearchDescriptorCopy.toVodDescriptor(descriptor));
        }

        return  descriptorList;
    }

    public static List<SearchDescriptorCopy> toSearchDescriptorList(List<VodDescriptor> descriptors) {

        ArrayList<SearchDescriptorCopy> searchDescriptorsList = new ArrayList<SearchDescriptorCopy>();

        for(VodDescriptor descriptor: descriptors) {
            searchDescriptorsList.add(new SearchDescriptorCopy(descriptor));
        }

        return searchDescriptorsList;
    }
    ////

    public SearchDescriptorCopy(VodDescriptor descriptor) {
        this(descriptor.getVodAddress(), descriptor.getAge(), descriptor.isConnected(), descriptor.getNumberOfIndexEntries(), false);
    }

    public SearchDescriptorCopy(VodAddress vodAddress) {
        this(vodAddress, false,0, false);
    }

    public SearchDescriptorCopy(VodAddress vodAddress, SearchDescriptorCopy searchDescriptor) {
        this(vodAddress, searchDescriptor.isConnected(), searchDescriptor.getNumberOfIndexEntries(), searchDescriptor.isLeaderGroupMember());
    }

    public SearchDescriptorCopy(VodAddress vodAddress, int age, boolean connected, long numberOfIndexEntries, boolean isLeaderGroupMember){
        this(new OverlayAddress(vodAddress), connected , numberOfIndexEntries, isLeaderGroupMember);
    }

    public SearchDescriptorCopy(VodAddress vodAddress, boolean connected, long numberOfIndexEntries, boolean isLeaderGroupMember) {
        this(new OverlayAddress(vodAddress), connected , numberOfIndexEntries, isLeaderGroupMember);
    }

    public SearchDescriptorCopy(SearchDescriptorCopy descriptor){
        this(descriptor.getOverlayAddress(), descriptor.isConnected(), descriptor.getNumberOfIndexEntries(), descriptor.isLeaderGroupMember());
    }

    public SearchDescriptorCopy(OverlayAddress overlayAddress, boolean connected, long numberOfIndexEntries, boolean isLeaderGroupMember){
        this.overlayAddress = overlayAddress;
        this.connected = connected;
        this.numberOfIndexEntries = numberOfIndexEntries;
        this.isLeaderGroupMember = isLeaderGroupMember;
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

    
    public boolean isLeaderGroupMember(){
        return this.isLeaderGroupMember;
    }
    
    @Override
    public String toString() {
        return (this.overlayAddress.toString() + " entries: " + this.getNumberOfIndexEntries());
    }

    public OverlayAddress getOverlayAddress() {
        return overlayAddress;
    }


    @Override
    public int compareTo(SearchDescriptorCopy that) {

        if(that == null){
            throw new RuntimeException("Can't compare to a null descriptor.");
        }

        if(equals(that)){
            return 0;
        }

        if(this.getOverlayAddress() == null || that.getOverlayAddress() == null){
            throw new IllegalArgumentException(" Parameters to compare are null");
        }
        
        
        if(this.isLeaderGroupMember != that.isLeaderGroupMember){
            
            if(this.isLeaderGroupMember){
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
        SearchDescriptorCopy other = (SearchDescriptorCopy) obj;

        if (this.overlayAddress == null) {
            if (other.getOverlayAddress() != null) {
                return false;
            }
        } else if (other.overlayAddress == null) {
            return false;
        }

        if (!this.overlayAddress.equals(other.overlayAddress)) {
            return false;
        }
        else if(!(this.numberOfIndexEntries == other.numberOfIndexEntries)){
            return false;
        }
        
        return (this.isLeaderGroupMember == other.isLeaderGroupMember);
    }

    public long getNumberOfIndexEntries() {
        return numberOfIndexEntries;
    }

    /**
     * Fetch the original partitioning depth.
     *
     * @return originalPartitioning Depth.
     */
    public int getReceivedPartitionDepth(){
        return this.overlayAddress.getPartitionIdDepth();
    }

    
    public void setLeaderGroupMember(boolean memberStatus){
        isLeaderGroupMember = memberStatus;
    }
    
    
    @Override
    public SearchDescriptorCopy deepCopy() {
        return new SearchDescriptorCopy(this.getOverlayAddress(), this.isConnected(), this.getNumberOfIndexEntries(), this.isLeaderGroupMember);
    }
}