package se.sics.ms.util;

import se.sics.gvod.net.VodAddress;

/**
 * Helper class for the manipulating and calculating the overlayId.
 * Created by babbar on 2015-03-25.
 */
public class OverlayIdHelper {


    /**
     * Create overlay id for the values passed in the function.
     * @param partitioningType Partitioning Type
     * @param partitionIdDepth Partition Depth
     * @param partitionId Partition Id
     * @param categoryId Category Id
     * @return overlayId.
     */
    public static int encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType partitioningType,
                                                            int partitionIdDepth, int partitionId, int categoryId) {

        // ========= NOTE: Changed the minimum partition depth to 0.
        if(partitionIdDepth > 15 || partitionIdDepth < 0)
            throw new IllegalArgumentException("partitionIdDepth must be between 0 and 15");
        if(partitionId > 1023 || partitionId < 0)
            throw new IllegalArgumentException("partitionId must be between 0 and 1023");
        if(categoryId > 65535 || categoryId < 0)
            throw new IllegalArgumentException("categoryId must be between 0 and 65535");

        int result = partitioningType.ordinal() << 30;
        result = result | (partitionIdDepth << 26);
        result = result | (partitionId << 16);
        result = result | categoryId;

        return result;
    }


    /**
     * Calculate the category id based on the overlay id.
     * @param overlayId OverlayId
     * @return CategoryId
     */
    public static int getCategoryId(int overlayId) {
        return overlayId & 65535;
    }

    /**
     * Calculate the partition id based on the overlay id.
     * @param overlayId OverlayId
     * @return Partition Id
     */
    public static int getPartitionId(int overlayId) {
        return (overlayId & 67043328) >>> 16;
    }

    /**
     * Calculate the partition depth from the overlay id.
     * @param overlayId OverlayId
     * @return Partitioning Depth
     */
    public static int getPartitionIdDepth(int overlayId) {
        return (overlayId & 1006632960) >>> 26;
    }

    /**
     * Calculate the partitioning type from overlay id.
     * @param overlayId overlayid
     * @return Partitioning Type
     */
    public static VodAddress.PartitioningType getPartitioningType(int overlayId) {
        return VodAddress.PartitioningType.values()[(overlayId & -1073741824) >>> 30];
    }

}
