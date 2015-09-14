package se.sics.ms.simulation;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.ktoolbox.cc.sim.CCSimMainInit;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.main.SimulatorHostCompInit;
import se.sics.ms.search.PeerInit;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.helper.SystemConfigBuilder;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Utility class to help start the nodes in the system.
 * <p/>
 * Created by babbarshaer on 2015-03-01.
 */
public class SweepOperationsHelper {

    private final static HashMap<Long, DecoratedAddress> peersAddressMap;
    private final static ConsistentHashtable<Long> ringNodes;
    private static Map<Integer, TreeSet<Integer>> partitionNodeMap;
    private static Map<Integer, TreeSet<Integer>> partitionNodeMapCopy;
    private final static CroupierConfig croupierConfiguration;
    private final static SearchConfiguration searchConfiguration;
    private final static GradientConfiguration gradientConfiguration;
    private final static ChunkManagerConfig chunkManagerConfiguration;
    private final static GradientConfig gradientConfig;
    private final static ElectionConfig electionConfig;
    private static SystemConfig systemConfig;
    private final static TreeGradientConfig treeGradientConfig;

    private static Config config;
    private static DecoratedAddress caracalClientAddress;
    private static SystemConfigBuilder builder;
    private static Logger logger = LoggerFactory.getLogger(SweepOperationsHelper.class);
    private static Long identifierSpaceSize;
    private static DecoratedAddress bootstrapAddress = null;
    private static int counter = 0;
    private static List<DecoratedAddress> bootstrapNodes = new ArrayList<DecoratedAddress>();
    private static InetAddress ip;
    private static int port;
    private static List<Long> reservedIdList;
    private static int baseSeed = 100;

    static {

//        int startId = 128;
//        int currentId = startId;
//        BasicSerializerSetup.registerBasicSerializers(currentId);
//        currentId += BasicSerializerSetup.serializerIds;
//        currentId = CroupierSerializerSetup.registerSerializers(currentId);
//        currentId = GradientSerializerSetup.registerSerializers(currentId);
//        currentId = ElectionSerializerSetup.registerSerializers(currentId);
//        currentId = AggregatorSerializerSetup.registerSerializers(currentId);
//        currentId = ChunkManagerSerializerSetup.registerSerializers(currentId);
//        SerializerSetup.registerSerializers(currentId);


        reservedIdList = new ArrayList<Long>();
        reservedIdList.add((long) 0);

        config = ConfigFactory.load("application.conf");

        identifierSpaceSize = new Long(3000);
        peersAddressMap = new HashMap<Long, DecoratedAddress>();
        ringNodes = new ConsistentHashtable<Long>();

        partitionNodeMap = new HashMap<Integer, TreeSet<Integer>>();
        croupierConfiguration = new CroupierConfig(config);
        searchConfiguration = SearchConfiguration.build();
        gradientConfiguration = GradientConfiguration.build();
        chunkManagerConfiguration = new ChunkManagerConfig(config);
        gradientConfig = new GradientConfig(config);
        electionConfig = new ElectionConfig(config);
        treeGradientConfig = new TreeGradientConfig(config);
        try {
            ip = InetAddress.getLocalHost();
            port = 9999;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Take an id and check if there would be any conflicts with previous stored id's.
     *
     * @return node id for the peer.
     */
    public static long getStableId(long id) {

        Long successor = ringNodes.getNode(id);

        while ((successor != null && successor.equals(id)) || reservedIdList.contains(successor)) {
            id = (id + 1) % identifierSpaceSize;
            successor = ringNodes.getNode(id);
        }

        if (id == Integer.MIN_VALUE) {
            logger.error("Call to generate the lowest id. ");
        }
        return id;
    }


    /**
     * Based on the NodeId provided, generate an init configuration for the search peer.
     *
     * @param id NodeId
     */
    public static SearchPeerInit generatePAGPeerInit(DecoratedAddress simulatorAddress, Set<DecoratedAddress> bootstrap, long id) {

        logger.trace(" Generating address for peer with id: {} ", id);

        DecoratedAddress decoratedAddress = DecoratedAddress.open(ip, port, (int)id);
        Optional<DecoratedAddress> simAddress  = simulatorAddress != null

                ? Optional.of(simulatorAddress)
                : Optional.<DecoratedAddress>absent();

        systemConfig = new SystemConfig(config, baseSeed + id, decoratedAddress, Optional.<Address>absent(), simAddress);
        SearchPeerInit init = new SearchPeerInit(systemConfig, croupierConfiguration, searchConfiguration, gradientConfiguration, chunkManagerConfiguration, gradientConfig, electionConfig, treeGradientConfig);

        ringNodes.addNode(id);
        peersAddressMap.put(id, systemConfig.self);

        bootstrapNodes = new ArrayList<DecoratedAddress>();
        bootstrapNodes.add(systemConfig.self);

        return init;
    }


    /**
     * Based on the NodeId provided, generate an init configuration for the search peer.
     *
     * @param id NodeId
     */
    public static PeerInit generatePALPeerInit(DecoratedAddress simulatorAddress, Set<DecoratedAddress> bootstrap, long id) {

        logger.trace(" Generating address for peer with id: {} ", id);

        DecoratedAddress decoratedAddress = DecoratedAddress.open(ip,port, (int)id);
        Optional<DecoratedAddress> simAddress  = simulatorAddress != null

                ? Optional.of(simulatorAddress)
                : Optional.<DecoratedAddress>absent();

        systemConfig = new SystemConfig(config, baseSeed + id, decoratedAddress, Optional.<Address>absent(), simAddress);

        ApplicationSelf applicationSelf = new ApplicationSelf(decoratedAddress);
        PeerInit init = new PeerInit(applicationSelf, systemConfig, croupierConfiguration, searchConfiguration, gradientConfiguration, chunkManagerConfiguration, gradientConfig, electionConfig, treeGradientConfig);

        ringNodes.addNode(id);
        peersAddressMap.put(id, applicationSelf.getAddress());

        bootstrapNodes = new ArrayList<DecoratedAddress>();
        bootstrapNodes.add(applicationSelf.getAddress());

        return init;
    }


    /**
     * Based on the id passed, locate the next successor on the ring
     * and return the address.
     *
     * @param id Random Id
     * @return
     */
    public static DecoratedAddress getNodeAddressToCommunicate(Long id) {

        logger.debug(" Fetching random node address from the map. ");
        Long successor = ringNodes.getNode(id);

        DecoratedAddress address = peersAddressMap.get(successor);
        if (address == null) {
            throw new RuntimeException(" Unable to locate node to add index entry to.");
        }

        return address;
    }


    private static int partitionBucketId = 0;

    /**
     * Bucket to communicate to.
     *
     * @param id
     * @return
     */
    public static DecoratedAddress getBucketNodeToAddEntry(long id) {

        DecoratedAddress address = null;

        if (partitionNodeMapCopy.size() > 0) {

            if (partitionBucketId >= partitionNodeMapCopy.size()) {
                partitionBucketId = 0;
            }

            List<Integer> bucketIds = new ArrayList<Integer>(partitionNodeMapCopy.get(partitionBucketId));

            if (bucketIds.size() > 0) {

                address = peersAddressMap.get((long) bucketIds.get(random.nextInt(bucketIds.size())));
                if (address == null) {
                    throw new RuntimeException("Unable to find the bucket node in the base map");
                }

                logger.debug("Returning node for adding entry: {} from bucket: {}", address.getId(), partitionBucketId);
            } else {
                logger.warn("{}: Not enough nodes in a partition bucket, falling back to default");
                address = getNodeAddressToCommunicate(id);
            }

            partitionBucketId++; // Get the node from the next partition bucket now.
        } else {
            logger.warn("{}: Returning nodes from default pool.");
            address = getNodeAddressToCommunicate(id);
        }

        return address;
    }

    /**
     * Generate an instance of Index Entry.
     *
     * @return Junk Index Entry.
     */
    public static IndexEntry generateIndexEntry() {

        IndexEntry index = new IndexEntry(" ", "sweep test url", randomText(), 0, new Date(), "english", MsConfig.Categories.Video, "sweep test desc");
        index.setLeaderId(null);

        return index;
    }


    /**
     * Generate the constant search pattern.
     * For now generate a constant search pattern.
     * <p/>
     * <b>CAUTION:</b> Constant Search Pattern.
     *
     * @return SearchPattern
     */
    public static SearchPattern generateSearchPattern() {
        SearchPattern searchPattern = new SearchPattern("sweep", 0, 0, null, null, null, MsConfig.Categories.Video, null);
        return searchPattern;
    }


    /**
     * Generate random string.
     *
     * @return Random string.
     */
    private static String randomText() {

        StringBuilder sb = new StringBuilder();
        sb.append("SweepEntry" + counter);
        counter++;

        return sb.toString();
    }


    public static DecoratedAddress getBasicAddress(long id) {
        logger.info("C a l l e d .. .. .. " + id);
        return peersAddressMap.get(id);
    }

    private static Integer randomPartitionBucket;
    private static List<Integer> partitionBucketNodes;
    private static Random random = new Random(100);


    public static long getPartitionBucketNode(long id) {

        if ((partitionBucketNodes == null || partitionBucketNodes.isEmpty())) {

            partitionNodeMap.remove(randomPartitionBucket);

            if (partitionNodeMap.size() > 0) {

                Map.Entry<Integer, TreeSet<Integer>> entry = partitionNodeMap.entrySet().iterator().next();
                randomPartitionBucket = entry.getKey();
                logger.debug("Random Partition Bucket Info: {}", randomPartitionBucket);
                partitionBucketNodes = new ArrayList<Integer>(entry.getValue());
                logger.error("Partition Bucket Nodes for partition:{} : {}", randomPartitionBucket, partitionBucketNodes);
            } else {

                logger.warn("Returning nodes not part of partition bucket. ");
                logger.warn("Partition Bucket: {}", partitionBucketNodes);
                return getStableId(id);
            }

        }


        if (partitionBucketNodes.isEmpty()) {
            String str = "Unable to find bucket nodes for partition: " + randomPartitionBucket;
            throw new RuntimeException(str);
        }

        return partitionBucketNodes.remove(0);
    }


    /**
     * Based on the parameters generate an equal sized node entries per partition.
     *
     * @param depth      partition depth.
     * @param bucketSize size of bucket.
     */
    public static void generateNodesPerPartition(long depth, long bucketSize, int seed) {

        partitionNodeMap = PartitionOperationsHelper.generateNodeList((int) depth, (int) bucketSize, new Random(seed));
        partitionNodeMapCopy = PartitionOperationsHelper.generateNodeList((int) depth, (int) bucketSize, new Random(seed));

        if (partitionNodeMap.isEmpty()) {
            throw new RuntimeException(" Unable to generate partition buckets.");
        }

        logger.debug("Partition Node Map : {}", partitionNodeMap);
    }

    public static DecoratedAddress getAggregatorAddress() {
        return DecoratedAddress.open(ip, port, 0);
    }


    /**
     * Access the ring node structure and then determine the node to kill.
     * * @return
     */
    public static Integer removeNode(long id) {

        long result;
        result = ringNodes.getNode(id);
        while (result < 0) {

            id = (id + 1) % identifierSpaceSize;
            result = ringNodes.getNode(id);
        }

        ringNodes.removeNode(result);
        return (int) result;
    }


    static long leaderGroupNodeId = Integer.MIN_VALUE;

    public static long getLeaderGroupNodeId(Long id) {
        return ++leaderGroupNodeId;
    }


    /**
     * The Tree Set picks up the first node from the bucket i.e. the expected leader node.
     *
     * @param bucketId
     * @return
     */
    public static DecoratedAddress getNodeForBucket(long bucketId) {

        if (partitionNodeMapCopy == null) {
            throw new RuntimeException("Bucket Map empty");
        }

        TreeSet<Integer> bucketNodes = partitionNodeMapCopy.get((int) bucketId);

        if (bucketNodes == null || bucketNodes.isEmpty()) {
            throw new RuntimeException("No nodes for the bucket found");
        }

        int nodeId = bucketNodes.first();
        return peersAddressMap.get((long) nodeId);
    }


    /**
     * The Tree Set picks up the first node from the bucket i.e. the expected leader node.
     *
     * @param bucketId
     * @return
     */
    public static DecoratedAddress getRandomNodeForBucket(long bucketId) {

        if (partitionNodeMapCopy == null) {
            throw new RuntimeException("Bucket Map empty");
        }

        List<Integer> bucketNodes = new ArrayList<Integer>(partitionNodeMapCopy.get((int) bucketId));

        if (bucketNodes == null || bucketNodes.isEmpty()) {
            throw new RuntimeException("No nodes for the bucket found");
        }

        int nodeId = bucketNodes.get(random.nextInt(bucketNodes.size()));
        return peersAddressMap.get((long) nodeId);
    }


    public static class PartitionOperationsHelper {

        public static Map<Integer, TreeSet<Integer>> generateNodeList(int depth, int bucketSize, Random random) {

            Map<Integer, TreeSet<Integer>> nodeList = new HashMap<Integer, TreeSet<Integer>>();
            List<Boolean> filledBuckets = new ArrayList<Boolean>();
            int maxSize = (int) Math.pow(2, depth);

            while (filledBuckets.size() < maxSize) {

                int id = random.nextInt();
                int genBucketId = generateBucketId(id, depth);
                TreeSet<Integer> idSet = nodeList.get(genBucketId);

                if (idSet == null) {
                    idSet = new TreeSet<Integer>();
                    nodeList.put(genBucketId, idSet);
                } else if (idSet.size() == bucketSize) {
                    continue;
                }

                idSet.add(id);
                if (idSet.size() == bucketSize) {
                    filledBuckets.add(true);
                }
            }

            return nodeList;
        }


        private static int generateBucketId(int nodeId, int depth) {

            int partition = 0;
            for (int i = 0; i < depth; i++) {
                partition = partition | (nodeId & (1 << i));
            }

            return partition;
        }


    }


//  Caracal host component connections and other related methods.


    /**
     * Construct initialization class for the host component
     * used to start the node in simulation.
     * @return
     */
    public static SimulatorHostCompInit getSimHostCompInit(DecoratedAddress simulatorAddress, Set<DecoratedAddress> bootstrap, long id){

        logger.error("Creating Node: - > {}", id);

        DecoratedAddress selfAddress = DecoratedAddress.open(ip, port, (int)id);
        storePeerAddress(selfAddress);

        Optional<DecoratedAddress> simAddress  = simulatorAddress != null

                ? Optional.of(simulatorAddress)
                : Optional.<DecoratedAddress>absent();

        SystemConfig systemConfig = new SystemConfig(config, baseSeed + id, selfAddress, Optional.<Address>absent(), simAddress);
        return new SimulatorHostCompInit(caracalClientAddress, systemConfig, config);
    }


    /**
     * Get the init for the caracal client.
     * @param id
     * @return
     */
    public static CCSimMainInit getCCSimInit(long id){

//      Get the basic address for the caracal sim client.
        DecoratedAddress decoratedAddress = DecoratedAddress.open(ip, port, (int)id);

//      Update the address store.
        storePeerAddress(decoratedAddress);

        return new CCSimMainInit(20, decoratedAddress);
    }


    /**
     * Store the peer address generated in a local store
     * which would be used later.
     *
     * @param address client address.
     */
    private static void storePeerAddress(DecoratedAddress address){
        peersAddressMap.put(Long.valueOf(address.getId()), address);
    }


    /**
     * Based on the identifier, fetch the peer address from the
     * data store.
     *
     * @param id identifier.
     * @return Address
     */
    public static DecoratedAddress getPeerAddress(long id){
        return peersAddressMap.get(id);
    }

    /**
     * Store the address of the peer that will be used as the caracal client.
     * @param address address
     */
    public static void storeCaracalSimClientAddress(DecoratedAddress address){

        caracalClientAddress = address;
        logger.debug(" !!!!!!! Storing the caracal client address as : {}", caracalClientAddress);
    }
}
