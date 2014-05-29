package se.sics.ms.util;

import org.junit.*;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.types.PartitionId;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/22/13
 * Time: 2:20 PM
 */
public class UtilTests {
    private static InetAddress inetAddress = null;
    private static Address address;
    private static VodAddress vodAddress;
    private static VodDescriptor vodDescriptor;
    private static MsSelfImpl self;


    public UtilTests() {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        inetAddress = null;
        try {
            inetAddress = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        address = new Address(inetAddress, 58027, 125);
        vodAddress = new VodAddress(address, VodConfig.SYSTEM_OVERLAY_ID);
        vodDescriptor = new VodDescriptor(vodAddress);
        self = new MsSelfImpl(vodAddress);
        self.setOverlayId(67108864);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void determineYourPartitionTest() {
        int partitionId = 0;

        boolean partition1 = PartitionHelper.determineYourNewPartitionSubId(1,
                new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 1, partitionId));
        assert (partition1);

        boolean partition2 = PartitionHelper.determineYourNewPartitionSubId(2,
                new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 1, partitionId));
        assert (!partition2);

    }

    @Test
    public void determineVodDescriptorPartitionTest() {
        PartitionId partitionId = new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 1, 0);
        PartitionId newPartitionId = new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE, 1, 1);

        PartitionHelper.setPartitionId(vodDescriptor.getVodAddress(), partitionId);

        PartitionId partition = PartitionHelper.determineVodDescriptorPartition(vodDescriptor, true, 1);

        assert(partition.equals(newPartitionId));

    }

    @Test
    public void adjustDescriptorsToNewPartitionIdTest() {
        PartitionId selfPartition = new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE,
                1, 1);

        int overlayId = VodAddress.encodePartitionDataAndCategoryIdAsInt(selfPartition.getPartitioningType(),
                selfPartition.getPartitionIdDepth(), selfPartition.getPartitionId(), 0);
        self.setOverlayId(overlayId);

        Address address1 = new Address(inetAddress, 58027, 126);
        VodAddress vodAddress1 = new VodAddress(address1, VodConfig.SYSTEM_OVERLAY_ID);
        VodDescriptor vodDescriptor1 = new VodDescriptor(vodAddress1);
        PartitionHelper.setPartitionId(vodDescriptor1.getVodAddress(), selfPartition);

        ArrayList<VodDescriptor> descriptors = new ArrayList<VodDescriptor>();
        descriptors.add(vodDescriptor);
        descriptors.add(vodDescriptor1);

        VodAddress selfAddress = self.getAddress();

        PartitionId partitionIdToAdjustTo = new PartitionId(selfAddress.getPartitioningType(),
                selfAddress.getPartitionIdDepth(), selfAddress.getPartitionId());
        PartitionHelper.adjustDescriptorsToNewPartitionId(partitionIdToAdjustTo, descriptors);

        assert (descriptors.size() == 1);
        assert (descriptors.get(0).equals(vodDescriptor));
    }
}
