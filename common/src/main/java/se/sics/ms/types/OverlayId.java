package se.sics.ms.types;

import se.sics.gvod.net.VodAddress;

/**
 * Created by alidar on 9/11/14.
 */
public class OverlayId implements Comparable<OverlayId> {

    protected int id;

    static public int getCategoryId(int overlayId) {
        return overlayId & 65535;
    }
    static public int getPartitionId(int overlayId) {
        return (overlayId & 67043328) >>> 16;
    }
    static public int getPartitionIdDepth(int overlayId) {
        return (overlayId & 1006632960) >>> 26;
    }
    static public VodAddress.PartitioningType getPartitioningType(int overlayId) {
        return VodAddress.PartitioningType.values()[(overlayId & -1073741824) >>> 30];
    }

    public  OverlayId(int overlayId) {
        this.id = overlayId;
    }

    public int getId() {
        return id;
    }

    public int getCategoryId() {
        return OverlayId.getCategoryId(this.id);
    }

    public int getPartitionId() {
        return OverlayId.getPartitionId(this.id);
    }

    public int getPartitionIdDepth() {
        return OverlayId.getPartitionIdDepth(this.id);
    }

    public VodAddress.PartitioningType getPartitioningType() {
        return OverlayId.getPartitioningType(this.id);
    }

    public int compareTo(OverlayId o) {
        if (equals(o)) {
            return 0;
        }
        OverlayId other = (OverlayId)o;

        if (this.id > other.getId()) {
            return 1;
        }
        return -1;
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
        OverlayId other = (OverlayId) obj;

        if (this.id != other.getId()) {
            return false;
        }

        return true;
    }

    public int hashCode() {
        final int prime = 7;
        int result = prime + (this.id * prime);
        return result;
    }

    @Override
    public String toString() {
        return Integer.toString(this.id);
    }
}
