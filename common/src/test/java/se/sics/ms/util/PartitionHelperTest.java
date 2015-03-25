package se.sics.ms.util;

import junit.framework.Assert;
import org.javatuples.*;
import org.javatuples.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.OverlayId;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.SearchDescriptor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Class to test the functionality of the partition helper class.
 *
 * Created by babbar on 2015-03-23.
 */
public class PartitionHelperTest {

    private static InetAddress ipAddress;
    private static Address defaultAddress;
    private static VodAddress defaultVodAddress;


    @BeforeClass
    public static void beforeClass() throws UnknownHostException {
        ipAddress = InetAddress.getLocalHost();
        defaultAddress = new Address(ipAddress, 10000, 0);
        defaultVodAddress = new VodAddress(defaultAddress, 0);
    }


    @Test
    public void getPartitionOtherHalfTest(){

        PartitionId partitionId  = new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 0 , 0);
        int otherPartitionId = PartitionHelper.getPartitionIdOtherHalf(partitionId);
        Assert.assertEquals(0, otherPartitionId);

        partitionId = new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE, 1, 0);
        otherPartitionId = PartitionHelper.getPartitionIdOtherHalf(partitionId);

        Assert.assertEquals(1,otherPartitionId);

        partitionId = new PartitionId(VodAddress.PartitioningType.MANY_BEFORE, 2 , 1);
        otherPartitionId = PartitionHelper.getPartitionIdOtherHalf(partitionId);

        Assert.assertEquals(3, otherPartitionId);
    }



//    @Test
//    public void removeOldBucketsTest() {
//
//        Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>> routingMap= init();
//        PartitionId testPartitionId = new PartitionId(VodAddress.PartitioningType.MANY_BEFORE, 2, 3);
//
//        PartitionHelper.removeOldBuckets(testPartitionId, routingMap);
//        Assert.assertEquals("Map size test", 1, routingMap.size());
//    }

    /**
     * Initialize Method.
     */
    private Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>> init() {

        int  ovid1 = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE, 1, 1, 0);
        int  ovid2 = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE, 1, 0, 0);
        
        Address ad1 = new Address(ipAddress, 9000, 10000);
        VodAddress vod1 = new VodAddress(ad1, 0);
        SearchDescriptor sd1 = new SearchDescriptor(vod1, ovid1);
        HashSet<SearchDescriptor> hs1 = new HashSet<SearchDescriptor>();
        hs1.add(sd1);
        
        Address ad2 = new Address(ipAddress, 10000, 110000);
        VodAddress vod2 = new VodAddress(ad2, 0);
        SearchDescriptor sd2 = new SearchDescriptor(vod2, ovid2);
        HashSet<SearchDescriptor> hs2 = new HashSet<SearchDescriptor>();
        hs2.add(sd2);
        
        Map<Integer, org.javatuples.Pair<Integer, HashSet<SearchDescriptor>>> categoryRoutingMap = new HashMap<Integer, Pair<Integer, HashSet<SearchDescriptor>>>();
        categoryRoutingMap.put(new Integer(1), Pair.with(1,hs1));
        
        categoryRoutingMap.put(0, Pair.with(1, hs2));
        return categoryRoutingMap;
    }


    @Test
    public void encodePartitionDataAsIntTest(){
        
        int partitionId = 3;
        int partitionDepth = 2;
        VodAddress.PartitioningType partitionType = VodAddress.PartitioningType.MANY_BEFORE;
        int categoryId = 0;

        int overlayIdInt = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(partitionType, partitionDepth, partitionId, categoryId);
        OverlayId overlayId = new OverlayId(overlayIdInt);
        
        
        Assert.assertEquals("partition id check", partitionId, overlayId.getPartitionId());
        Assert.assertEquals("partition depth check", partitionDepth, overlayId.getPartitionIdDepth());
        Assert.assertEquals("category Id check", categoryId, overlayId.getCategoryId());
        Assert.assertEquals("partition type check", partitionType, overlayId.getPartitioningType());
    }


}
