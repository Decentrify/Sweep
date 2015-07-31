package se.sics.ms.types;

import se.sics.ms.util.PartitioningType;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Utility Class for representing the address in the system.
 * Created by alidar on 9/12/14.
 */
public class OverlayAddress implements Comparable<OverlayAddress>{

    private DecoratedAddress address;
    private OverlayId overlayId;

    public OverlayAddress(DecoratedAddress address, int overlayId) {
        this.address = address;
        this.overlayId = new OverlayId(overlayId);
    }

//    public OverlayAddress(DecoratedAddress address) {
//        this(address, address.getOverlayId());
//    }

    public int getId() {
        return address.getId();
    }

    public int getCategoryId() {
        return overlayId.getCategoryId();
    }

    public int getPartitionId() {
        return overlayId.getPartitionId();
    }

    public int getPartitionIdDepth() {
        return overlayId.getPartitionIdDepth();
    }

    public PartitioningType getPartitioningType() {
        return overlayId.getPartitioningType();
    }

    @Override
    public int compareTo(OverlayAddress other) {
        if (equals(other)) {
            return 0;
        }

        int res = -1 * (this.address.getId().compareTo(other.getAddress().getId()));

        if(res != 0)
            return  res;
        else
            return this.overlayId.compareTo(other.getOverlayId());
    }

    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if(! (obj instanceof OverlayAddress))
            return false;

        OverlayAddress other = (OverlayAddress) obj;
        if(! (this.address == null ? other.address == null : this.address.equals(other.address)))
            return false;

        return (this.overlayId == null ? other.overlayId == null : this.overlayId.equals(other.overlayId));
    }

    public int hashCode() {
        final int prime = 7;
        int result = prime + ((this.address == null) ? 0 : this.address.hashCode())  +
                ((this.overlayId == null) ? 0 : this.overlayId.hashCode());
        return result;
    }

    public OverlayId getOverlayId() {
        return overlayId;
    }

    public DecoratedAddress getAddress() {
        return address;
    }

    @Override
    public String toString() {
        //return this.getAddress().toString() + ", overlayId: " + (overlayId.toString());

        return (this.address == null ? "" : this.address) + ":" + overlayId;
    }
}
