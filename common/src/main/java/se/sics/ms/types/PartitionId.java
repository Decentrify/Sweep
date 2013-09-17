package se.sics.ms.types;

import se.sics.gvod.net.VodAddress;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 9/8/13
 * Time: 9:55 AM
 */
public class PartitionId {
    private VodAddress.PartitioningType partitioningType;
    private int partitionIdDepth;
    private int partitionId;

    public PartitionId(VodAddress.PartitioningType partitioningType, int partitionIdDepth, int partitionId) {
        this.partitioningType = partitioningType;
        this.partitionIdDepth = partitionIdDepth;
        this.partitionId = partitionId;
    }

    public VodAddress.PartitioningType getPartitioningType() {
        return partitioningType;
    }

    public void setPartitioningType(VodAddress.PartitioningType partitioningType) {
        this.partitioningType = partitioningType;
    }

    public int getPartitionIdDepth() {
        return partitionIdDepth;
    }

    public void setPartitionIdDepth(int partitionIdDepth) {
        this.partitionIdDepth = partitionIdDepth;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionId that = (PartitionId) o;

        if (partitionId != that.partitionId) return false;
        if (partitionIdDepth != that.partitionIdDepth) return false;
        if (partitioningType != that.partitioningType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitioningType.hashCode();
        result = 31 * result + partitionIdDepth;
        result = 31 * result + partitionId;
        return result;
    }

    public LinkedList<Boolean> getAsLinkedList() {
        LinkedList<Boolean> asList = new LinkedList<Boolean>();

        if(partitioningType == VodAddress.PartitioningType.NEVER_BEFORE) {
            asList.add(false);
            return asList;
        }

        for(int i=0; i<partitionIdDepth; i++) {
            boolean partId = (partitionId & (1 << i)) != 0;
            asList.add(partId);
        }

        return asList;
    }

    @Override
    public String toString() {
        return getAsLinkedList().toString();
    }
}
