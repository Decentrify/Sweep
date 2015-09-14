package se.sics.ms.common;

import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.OverlayIdHelper;
import se.sics.ms.util.PartitioningType;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 *
 * The class contains information about the node in the system
 * in terms of the peer address, utility and other necessary information.
 *
 * Created by babbar on 2015-04-17.
 */
public class ApplicationSelf {

    private DecoratedAddress address;
    private int overlayId;
    private boolean isLGMember;
    private long numberOfEntries;
    private long epochContainerEntries;
    private long actualEntries;     // Entries excluding landing entries.
    private LeaderUnit lastLeaderUnit;
    
    public ApplicationSelf (DecoratedAddress address){

        this.address = address;
        this.overlayId = 0;
        this.isLGMember = false;
        this.numberOfEntries = 0;
        this.epochContainerEntries = 0;
        this.actualEntries = 0;
    }

    public ApplicationSelf(DecoratedAddress address, int overlayId, boolean isLGMember, long numberOfEntries, long epochContainerEntries , long actualEntries, LeaderUnit lastLeaderUnit){

        this.address = address;
        this.overlayId = overlayId;
        this.isLGMember = isLGMember;
        this.numberOfEntries = numberOfEntries;
        this.epochContainerEntries = epochContainerEntries;
        this.actualEntries = actualEntries;
        this.lastLeaderUnit = lastLeaderUnit;
    }

    @Override
    public String toString() {
        return "ApplicationSelf{" +
                "address=" + address +
                ", overlayId=" + overlayId +
                ", isLGMember=" + isLGMember +
                ", numberOfEntries=" + numberOfEntries +
                ", epochContainerEntries=" + epochContainerEntries +
                ", actualEntries=" + actualEntries +
                ", lastLeaderUnit=" + lastLeaderUnit +
                '}';
    }


    public boolean isLGMember() {
        return isLGMember;
    }

    public void incrementECEntries(){
        this.epochContainerEntries ++;
    }

    public void resetContainerEntries(){
        this.epochContainerEntries = 0;
    }

    public long getEpochContainerEntries() {
        return epochContainerEntries;
    }

    public void setIsLGMember(boolean isLGMember){
        this.isLGMember = isLGMember;
    }

    public void incrementEntries(){
        this.numberOfEntries++;
    }

    public void setOverlayId(int overlayId){
        this.overlayId = overlayId;
    }

    public DecoratedAddress getAddress(){
        return this.address;
    }

    public long getNumberOfEntries(){
        return this.numberOfEntries;
    }

    public int getId(){
        return this.address.getId();
    }

    public int getOverlayId(){
        return this.overlayId;
    }

    public int getCategoryId(){
        return OverlayIdHelper.getCategoryId(this.overlayId);
    }

    public int getPartitionId(){
        return OverlayIdHelper.getPartitionId(this.overlayId);
    }

    public PartitioningType getPartitioningType(){
        return OverlayIdHelper.getPartitioningType(this.overlayId);
    }

    public int getPartitioningDepth(){
        return OverlayIdHelper.getPartitionIdDepth(this.overlayId);
    }
    /**
     * Construct a descriptor based on the information contained in the object.
     * @return Self Descriptor.
     */
    public PeerDescriptor getSelfDescriptor(){
        return new PeerDescriptor(new OverlayAddress(this.address, this.overlayId), false, this.numberOfEntries, this.isLGMember, this.lastLeaderUnit);
    }

    /**
     * A convenience constructor method for creating a shallow copy of the application object.
     * @return Copy of Self.
     */
    public ApplicationSelf shallowCopy(){
        return new ApplicationSelf(this.address, this.overlayId, this.isLGMember, this.numberOfEntries, this.epochContainerEntries, this.actualEntries, this.lastLeaderUnit);
    }

    public void setNumberOfEntries(long entries){
        this.numberOfEntries = entries;
    }

    public void incrementActualEntries(){
        this.actualEntries ++;
    }
    
    public long getActualEntries() {
        return actualEntries;
    }

    public void setActualEntries(long actualEntries) {
        this.actualEntries = actualEntries;
    }

    public void setLastLeaderUnit(LeaderUnit lastLeaderUnit) {
        this.lastLeaderUnit = lastLeaderUnit;
    }

    public void setSelfAddress(DecoratedAddress selfAddress){
        this.address = selfAddress;
    }
}
