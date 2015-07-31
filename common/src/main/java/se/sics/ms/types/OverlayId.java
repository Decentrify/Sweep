package se.sics.ms.types;

import se.sics.ms.util.OverlayIdHelper;
import se.sics.ms.util.PartitioningType;

/**
 * Handling the overlay information of the node in the system.
 * @author babbar
 */
public class OverlayId implements Comparable<OverlayId> {


    public int categoryId;
    public int partitionId;
    public int partitionIdDepth;
    public PartitioningType partitioningType;        //CAUTION: This filed needs to be removed eventually as the information is redundant but being used everywhere.


    public OverlayId(int overlayId){

        this.categoryId = OverlayIdHelper.getCategoryId(overlayId);
        this.partitionId = OverlayIdHelper.getPartitionId(overlayId);
        this.partitionIdDepth = OverlayIdHelper.getPartitionIdDepth(overlayId);
        this.partitioningType = OverlayIdHelper.getPartitioningType(overlayId);

    }

    public OverlayId(int categoryId, int partitionId, int partitionIdDepth, PartitioningType partitioningType){
        this.categoryId = categoryId;
        this.partitionId = partitionId;
        this.partitionIdDepth = partitionIdDepth;
        this.partitioningType = partitioningType;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getPartitionIdDepth() {
        return partitionIdDepth;
    }

    public PartitioningType getPartitioningType() {
        return partitioningType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OverlayId)) return false;

        OverlayId that = (OverlayId) o;

        if (categoryId != that.categoryId) return false;
        if (partitionIdDepth != that.partitionIdDepth) return false;
        if (partitionId != that.partitionId) return false;
//        if (partitioningType != that.partitioningType) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = categoryId;
        result = 31 * result + partitionId;
        result = 31 * result + partitionIdDepth;
        result = 31 * result + partitioningType.hashCode();
        return result;
    }

    /**
     * The compareTo method currently is not including PartitionType field check
     * because the field is equivalent to the partition depth and therefore doesn't
     * make any sense to include it.
     *
     * @param o
     * @return comparison result.
     */
    @Override
    public int compareTo(OverlayId o) {

        if(o == null){
            throw new IllegalArgumentException("Can't compare to null element");
        }

        int categoryCompareResult = Integer.valueOf(this.categoryId).compareTo(o.categoryId);
        if(categoryCompareResult != 0){
            return categoryCompareResult;
        }

        int partitionDepthCompareResult  = Integer.valueOf(this.partitionIdDepth).compareTo(o.partitionIdDepth);
        if(partitionDepthCompareResult != 0){
            return partitionDepthCompareResult;
        }

        return Integer.valueOf(this.partitionId).compareTo(o.partitionId);
    }

    @Override
    public String toString() {
        return "OverlayIdUpdated{" +
                "categoryId=" + categoryId +
                ", partitionId=" + partitionId +
                ", partitionDepth=" + partitionIdDepth +
                ", partitioningType=" + partitioningType +
                '}';
    }


    /**
     * Based on the information present in the class, convert it into int value through bit shifting.
     * @return combined int value.
     */
    public int getId(){
        return OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(this.partitioningType, this.partitionIdDepth, this.partitionId, this.categoryId);
    }
}
