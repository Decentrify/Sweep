package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.croupier.api.CroupierSelectionPolicy;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;
import se.sics.p2ptoolbox.gradient.core.GradientConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

/**
 * Utility class to help start the nodes in the system.
 *  
 * Created by babbarshaer on 2015-03-01.
 */
public class SweepOperationsHelper {

    private final static HashMap<Long, VodAddress> peersAddressMap;
    private final static ConsistentHashtable<Long> ringNodes;

    private final static CroupierConfig croupierConfiguration;
    private final static SearchConfiguration searchConfiguration;
    private final static GradientConfiguration gradientConfiguration;
    private final static ElectionConfiguration electionConfiguration;
    private final static ChunkManagerConfiguration chunkManagerConfiguration;
    private final static GradientConfig gradientConfig;
    
    private static Logger logger = LoggerFactory.getLogger(SweepOperationsHelper.class);
    private static Long identifierSpaceSize;
    private static VodAddress bootstrapAddress = null;
    private static int counter =0;
    
    static{
        identifierSpaceSize = new Long(3000);
        peersAddressMap  = new HashMap<Long, VodAddress>();
        ringNodes = new ConsistentHashtable<Long>();

        CroupierSelectionPolicy hardcodedPolicy = CroupierSelectionPolicy.RANDOM;
        croupierConfiguration = new CroupierConfig(MsConfig.CROUPIER_VIEW_SIZE, MsConfig.CROUPIER_SHUFFLE_PERIOD,
                MsConfig.CROUPIER_SHUFFLE_LENGTH, hardcodedPolicy);
        searchConfiguration = SearchConfiguration.build();
        gradientConfiguration = GradientConfiguration.build();
        electionConfiguration = ElectionConfiguration.build();
        chunkManagerConfiguration = ChunkManagerConfiguration.build();
        gradientConfig= new GradientConfig(MsConfig.GRADIENT_VIEW_SIZE,MsConfig.GRADIENT_SHUFFLE_PERIOD, MsConfig.GRADIENT_SHUFFLE_LENGTH);
        
    }

    /**
     *  Take an id and check if there would be any conflicts with previous stored id's.
     *
     * @return node id for the peer.
     */
    public static long getStableId(long id){
        
        Long successor = ringNodes.getNode(id);
        
        while (successor != null && successor.equals(id)) {
            id = (id + 1) % identifierSpaceSize;
            successor = ringNodes.getNode(id);
        }
        
        return id;
    }


    /**
     * Based on the NodeId provided, generate an init configuration for the search peer.
     * @param id NodeId
     */
    public static SearchPeerInit generatePeerInit(VodAddress simulatorAddress, long id){
        
        logger.info(" Generating address for peer with id: {} ", id);
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Address address = new Address(ip, 9999, (int) id);
        Self self = new MsSelfImpl(new VodAddress(address,
                PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.NEVER_BEFORE, 0, 0, MsConfig.Categories.Video.ordinal())));

        SearchPeerInit init  = new SearchPeerInit(self,croupierConfiguration,searchConfiguration,gradientConfiguration,electionConfiguration,chunkManagerConfiguration,gradientConfig, bootstrapAddress, simulatorAddress);
        
        ringNodes.addNode(id);
        peersAddressMap.put(id, self.getAddress());
        bootstrapAddress = self.getAddress();
        
        return init;
    }


    /**
     * Based on the id passed, locate the next successor on the ring
     * and return the address.
     *
     * @param id Random Id 
     * @return
     */
    public static VodAddress getNodeAddressToCommunicate(Long id){
        
        logger.info(" Fetching random node address from the map. ");
        Long successor = ringNodes.getNode(id);
        
        VodAddress address = null;
        
        
        
        address = peersAddressMap.get(successor);
        
        if(address == null){
            throw new RuntimeException(" Unable to locate node to add index entry to.");
        }

        return address;
    }


    /**
     * Generate an instance of Index Entry.
     *
     * @return Junk Index Entry.
     */
    public static IndexEntry generateIndexEntry(){

        IndexEntry index = new IndexEntry("sweep test url", randomText(), new Date(), MsConfig.Categories.Video, "", "sweep test desc", "");
        index.setLeaderId(null);
        
        return index;
    }


    /**
     * Generate random string.
     * @return Random string.
     */
    private static String randomText(){
        
        StringBuilder sb = new StringBuilder();
        sb.append("SweepEntry" + counter);
        counter++;
        
        return sb.toString();
    }
}
