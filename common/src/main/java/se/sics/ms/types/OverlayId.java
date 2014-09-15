package se.sics.ms.types;

import se.sics.gvod.net.VodAddress;

/**
 * Created by alidar on 9/11/14.
 */
public class OverlayId implements Comparable<OverlayId> {

    protected int id;

    public  OverlayId(int overlayId) {
        this.id = overlayId;
    }

    public int getId() {
        return id;
    }

    public int getCategoryId() {
        return id & 65535;
    }

    public int getPartitionId() {
        return (id & 67043328) >>> 16;
    }

    public int getPartitionIdDepth() {
        return (id & 1006632960) >>> 26;
    }

    public VodAddress.PartitioningType getPartitioningType() {
        return VodAddress.PartitioningType.values()[(id & -1073741824) >>> 30];
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
