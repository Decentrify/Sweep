package se.sics.ms.types;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alidar on 8/11/14.
 */
public class SearchDescriptor implements DescriptorBase, Comparable<SearchDescriptor>, Serializable, PeerView {

    private int age;
    private transient boolean connected;
    private OverlayAddress overlayAddress;
    private final long numberOfIndexEntries;

    private int receivedPartitionDepth;

    //// Conversion functions
    // FIXME: Do we still need VodDescriptor. If not Fix it. ?
    public static VodDescriptor toVodDescriptor(SearchDescriptor searchDescriptor) {
        VodDescriptor descriptor = new VodDescriptor(searchDescriptor.getVodAddress(), searchDescriptor.getNumberOfIndexEntries());
        descriptor.setAge(searchDescriptor.getAge());
        descriptor.setConnected(searchDescriptor.isConnected());

        return descriptor;
    }

    public static List<VodDescriptor> toVodDescriptorList(List<SearchDescriptor> searchDescriptors)
    {
        ArrayList<VodDescriptor> descriptorList = new ArrayList<VodDescriptor>();

        for(SearchDescriptor descriptor: searchDescriptors) {
            descriptorList.add(SearchDescriptor.toVodDescriptor(descriptor));
        }

        return  descriptorList;
    }

    public static List<SearchDescriptor> toSearchDescriptorList(List<VodDescriptor> descriptors) {

        ArrayList<SearchDescriptor> searchDescriptorsList = new ArrayList<SearchDescriptor>();

        for(VodDescriptor descriptor: descriptors) {
            searchDescriptorsList.add(new SearchDescriptor(descriptor));
        }

        return searchDescriptorsList;
    }
    ////

    public SearchDescriptor(VodDescriptor descriptor) {
        this(descriptor.getVodAddress(), descriptor.getAge(), descriptor.isConnected(), descriptor.getNumberOfIndexEntries());
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress) {
        this(vodAddress, 0, false,0,0);
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, int age) {
        this(vodAddress, age, false,0,0);
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, SearchDescriptor searchDescriptor) {
        this(vodAddress, searchDescriptor.getAge(), searchDescriptor.isConnected(), searchDescriptor.getNumberOfIndexEntries(), searchDescriptor.getReceivedPartitionDepth());
    }

    public SearchDescriptor(VodAddress vodAddress, int age, boolean connected, long numberOfIndexEntries){
        this(new OverlayAddress(vodAddress), age, connected , numberOfIndexEntries);
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, int age, boolean connected, long numberOfIndexEntries, int receivedPartitionDepth) {
        this(new OverlayAddress(vodAddress), age, connected , numberOfIndexEntries, receivedPartitionDepth);
    }

    // Convenience Constructor to create the SearchDescriptor from the VodDescriptor and use same Partitioning Depth.
    public SearchDescriptor(OverlayAddress overlayAddress, int age, boolean connected, long numberOfIndexEntries){
        this(overlayAddress, age, connected, numberOfIndexEntries, overlayAddress.getPartitionIdDepth());
    }

    public SearchDescriptor(SearchDescriptor descriptor){
        this(descriptor.getOverlayAddress(), descriptor.getAge(), descriptor.isConnected(), descriptor.getNumberOfIndexEntries(), descriptor.getReceivedPartitionDepth());
    }

    public SearchDescriptor(OverlayAddress overlayAddress, int age, boolean connected, long numberOfIndexEntries, int receivedPartitionDepth){
        this.overlayAddress = overlayAddress;
        setAge(age);
        this.connected = connected;
        this.numberOfIndexEntries = numberOfIndexEntries;
        this.receivedPartitionDepth  = receivedPartitionDepth;
    }


    public VodAddress getVodAddress() {
        return this.overlayAddress.getAddress();
    }

    public int getAge() {
        return age;
    }

    public int getId() {
        return this.overlayAddress.getId();
    }

    public int incrementAndGetAge() {
        return ++age;
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
        return (this.overlayAddress.toString() + " entries: " + this.getNumberOfIndexEntries());
    }

    public OverlayAddress getOverlayAddress() {
        return overlayAddress;
    }

    public void setAge(int age) {
        if (age > 65535) {
            age = 65535;
        }
        if (age < 0) {
            age = 0;
        }
        this.age = age;
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

//        int partitionDepthCompareResult = Integer.compare(this.overlayAddress.getOverlayId().getPartitionIdDepth(), that.overlayAddress.getPartitionIdDepth());

        int overlayIdComparisonResult = this.overlayAddress.getOverlayId().compareTo(that.overlayAddress.getOverlayId());

        if(overlayIdComparisonResult != 0){
            return overlayIdComparisonResult;
        }

        int indexEntryCompareResult = Long.compare(this.numberOfIndexEntries, that.numberOfIndexEntries);

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

        if (this.overlayAddress.equals(other.overlayAddress)) {

            if (this.numberOfIndexEntries == other.numberOfIndexEntries) {
                return true;
            }
        }
        return false;
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
        return this.receivedPartitionDepth;
    }

    public void setReceivedPartitionDepth(int receivedPartitionDepth) {
        this.receivedPartitionDepth = receivedPartitionDepth;
    }
    
    @Override
    public SearchDescriptor deepCopy() {
        return new SearchDescriptor(this.getOverlayAddress(), this.getAge() , this.isConnected(), this.getNumberOfIndexEntries(), this.getReceivedPartitionDepth());
    }
}