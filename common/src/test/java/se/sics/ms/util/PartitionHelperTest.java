package se.sics.ms.util;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.SearchDescriptor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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



    @Test
    public void removeOldBucketsTest(){

    }


    /**
     * Based on the quantity supplied create test search descriptors.
     * @param quantity
     * @return
     */
    private HashSet<SearchDescriptor> createSearchDescriptorSet(int quantity, int seed){

        HashSet<SearchDescriptor> searchDescriptorSet = new HashSet<SearchDescriptor>();
        Random random = new Random(seed);

        while(quantity > 0){
            quantity--;
        }
        return searchDescriptorSet;
    }

}
