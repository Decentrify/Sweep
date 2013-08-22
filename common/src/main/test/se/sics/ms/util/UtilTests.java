package se.sics.ms.util;

import org.junit.*;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.common.MsSelfImpl;

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
        LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
        partitionId.addFirst(false);

        boolean partition1 = PartitionHelper.determineYourPartition(1, partitionId, true);
        assert (partition1);

        boolean partition2 = PartitionHelper.determineYourPartition(2, partitionId, true);
        assert (!partition2);

    }

    @Test
    public void determineVodDescriptorPartitionTest() {
        LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
        partitionId.addFirst(false);

        LinkedList<Boolean> newPartitionId = new LinkedList<Boolean>();
        newPartitionId.addFirst(true);

        vodDescriptor.setPartitionId(partitionId);

        LinkedList<Boolean> partition = PartitionHelper.determineVodDescriptorPartition(vodDescriptor, true, 1);

        assert(partition.equals(newPartitionId));

    }

    @Test
    public void adjustDescriptorsToNewPartitionIdTest() {
        LinkedList<Boolean> selfPartitionId = new LinkedList<Boolean>();
        selfPartitionId.add(true);

        self.setPartitionsNumber(2);
        self.setPartitionId(selfPartitionId);

        vodDescriptor.setPartitionId(selfPartitionId);
        vodDescriptor = new VodDescriptor(vodAddress);

        Address address1 = new Address(inetAddress, 58027, 126);
        VodAddress vodAddress1 = new VodAddress(address1, VodConfig.SYSTEM_OVERLAY_ID);
        VodDescriptor vodDescriptor1 = new VodDescriptor(vodAddress1);
        vodDescriptor1.setPartitionId(selfPartitionId);

        ArrayList<VodDescriptor> descriptors = new ArrayList<VodDescriptor>();
        descriptors.add(vodDescriptor);
        descriptors.add(vodDescriptor1);

        PartitionHelper.adjustDescriptorsToNewPartitionId(self, descriptors);

        assert (descriptors.size() == 1);
        assert (descriptors.get(0).equals(vodDescriptor));
    }
}
