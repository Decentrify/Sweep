package se.sics.ms.types;

import java.util.Comparator;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ms.util.PartitioningType;

/**
 * Utility Class for representing the address in the system.
 * Created by alidar on 9/12/14.
 */
public class OverlayAddress implements Comparable<OverlayAddress>{

    private KAddress address;
    private OverlayId overlayId;

    public OverlayAddress(KAddress address, int overlayId) {
        this.address = address;
        this.overlayId = new OverlayId(overlayId);
    }

//    public OverlayAddress(DecoratedAddress address) {
//        this(address, address.getOverlayId());
//    }

    public int getId() {
        return ((IntIdentifier)address.getId()).id;
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
        if(! (this.address == null ? other.address == null : this.address.getId().equals(other.address.getId())))
            return false;

        return (this.overlayId == null ? other.overlayId == null : this.overlayId.equals(other.overlayId));
    }

    public int hashCode() {
        final int prime = 7;
        int result = prime + ((this.address == null) ? 0 : this.address.getId().hashCode())  +
                ((this.overlayId == null) ? 0 : this.overlayId.hashCode());
        return result;
    }

    public OverlayId getOverlayId() {
        return overlayId;
    }

    public KAddress getAddress() {
        return address;
    }

    @Override
    public String toString() {
        //return this.getAddress().toString() + ", overlayId: " + (overlayId.toString());

        return (this.address == null ? "" : this.address) + ":" + overlayId;
    }
}
