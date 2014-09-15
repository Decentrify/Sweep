package se.sics.ms.types;

import se.sics.gvod.net.VodAddress;

/**
 * Created by alidar on 9/12/14.
 */
public class OverlayAddress implements Comparable<OverlayAddress>{

    private VodAddress address;
    private OverlayId overlayId;

    public OverlayAddress(VodAddress address, int overlayId) {
        this.address = address;
        this.overlayId = new OverlayId(overlayId);
    }

    public OverlayAddress(VodAddress address) {
        this(address, address.getOverlayId());
    }

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

    public VodAddress.PartitioningType getPartitioningType() {
        return overlayId.getPartitioningType();
    }

    @Override
    public int compareTo(OverlayAddress other) {
        if (equals(other)) {
            return 0;
        }

        int res = this.address.compareTo(other.getAddress());

        if(res != 0)
            return  res;
        else
            return this.overlayId.compareTo(other.getOverlayId());
    }

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
        OverlayAddress other = (OverlayAddress) obj;

        if (this.address == null) {
            if (other.getAddress() != null) {
                return false;
            }
        } else if (other.getAddress() == null) {
            return false;
        }

        if(!this.address.equals(other.getAddress()))
            return false;

        if (this.overlayId == null) {
            if (other.getOverlayId() != null) {
                return false;
            }
        } else if (other.getOverlayId() == null) {
            return false;
        }

        return this.overlayId.equals(other.getOverlayId());
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

    public VodAddress getAddress() {
        return address;
    }

    @Override
    public String toString() {
        //return this.getAddress().toString() + ", overlayId: " + (overlayId.toString());

        return (this.address == null ? "" : this.address) + ":" + overlayId;
    }
}
