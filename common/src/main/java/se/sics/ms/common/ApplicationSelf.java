package se.sics.ms.common;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.SearchDescriptor;
import se.sics.ms.util.OverlayIdHelper;
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

    public ApplicationSelf(DecoratedAddress address){

        this.address = address;
        this.overlayId = 0;
        this.isLGMember = false;
        this.numberOfEntries = 0;
    }

    public ApplicationSelf(DecoratedAddress address, int overlayId, boolean isLGMember, long numberOfEntries){

        this.address = address;
        this.overlayId = overlayId;
        this.isLGMember = isLGMember;
        this.numberOfEntries = numberOfEntries;
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

    public VodAddress.PartitioningType getPartitioningType(){
        return OverlayIdHelper.getPartitioningType(this.overlayId);
    }

    public int getPartitioningDepth(){
        return OverlayIdHelper.getPartitionIdDepth(this.overlayId);
    }
    /**
     * Construct a descriptor based on the information contained in the object.
     * @return Self Descriptor.
     */
    public SearchDescriptor getSelfDescriptor(){
        return new SearchDescriptor(new OverlayAddress(this.address, this.overlayId), false, this.numberOfEntries, this.isLGMember);
    }

    /**
     * A convenience constructor method for creating a shallow copy of the application object.
     * @return Copy of Self.
     */
    public ApplicationSelf shallowCopy(){
        return new ApplicationSelf(this.address, this.overlayId, this.isLGMember, this.numberOfEntries);
    }

    public void setNumberOfEntries(long entries){
        this.numberOfEntries = entries;
    }

}
