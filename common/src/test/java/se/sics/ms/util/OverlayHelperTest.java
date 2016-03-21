package se.sics.ms.util;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Class for the Overlay Id Helper.
 *
 * Created by babbar on 2015-03-25.
 */
public class OverlayHelperTest {

    public static int partitionId;
    public static int partitionDepth;
    public static PartitioningType partitioningType;
    public static int categoryId;
    public static int overallId;

    @BeforeClass
    public static void beforeClass(){

        partitionId = 3;
        partitionDepth = 2;
        categoryId=0;
        partitioningType = PartitioningType.MANY_BEFORE;
        overallId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(partitioningType, partitionDepth, partitionId, categoryId);
    }

    @Test
    public void testCategoryId() {
        int createdCategoryId = OverlayIdHelper.getCategoryId(overallId);
        Assert.assertEquals("Category Comparison Test", categoryId, createdCategoryId);
    }

    @Test
    public void testPartitionId() {
        int createdPartitionId = OverlayIdHelper.getPartitionId(overallId);
        Assert.assertEquals("PartitionId Comparison Test", partitionId, createdPartitionId);
    }

    @Test
    public void testPartitionDepth() {
        int createdPartitionDepth = OverlayIdHelper.getPartitionIdDepth(overallId);
        Assert.assertEquals("Partition Depth Comparison Test", partitionDepth, createdPartitionDepth);
    }


    @Test
    public void testPartitionType() {
        PartitioningType createdPartitionType = OverlayIdHelper.getPartitioningType(overallId);
        Assert.assertEquals("Partition Type Comparison Test", partitioningType, createdPartitionType);
    }


}
