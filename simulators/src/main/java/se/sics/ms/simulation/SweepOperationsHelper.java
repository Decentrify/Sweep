package se.sics.ms.simulation;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Utility class to help start the nodes in the system.
 *  
 * Created by babbarshaer on 2015-03-01.
 */
public class SweepOperationsHelper {

    private final static HashMap<Long, DecoratedAddress> peersAddressMap;
    private final static ConsistentHashtable<Long> ringNodes;

    private final static CroupierConfig croupierConfiguration;
    private final static SearchConfiguration searchConfiguration;
    private final static GradientConfiguration gradientConfiguration;
    private final static ElectionConfiguration electionConfiguration;
    private final static ChunkManagerConfig chunkManagerConfiguration;
    private final static GradientConfig gradientConfig;
    private final static ElectionConfig electionConfig;
    private static SystemConfig systemConfig;

    private static Logger logger = LoggerFactory.getLogger(SweepOperationsHelper.class);
    private static Long identifierSpaceSize;
    private static DecoratedAddress bootstrapAddress = null;
    private static int counter =0;
    private static List<DecoratedAddress> bootstrapNodes = new ArrayList<DecoratedAddress>();
    private static InetAddress ip;
    private static int port;
    static{

        Config config = ConfigFactory.load("application.conf");

        identifierSpaceSize = new Long(3000);
        peersAddressMap  = new HashMap<Long, DecoratedAddress>();
        ringNodes = new ConsistentHashtable<Long>();

        croupierConfiguration = new CroupierConfig(config);
        searchConfiguration = SearchConfiguration.build();
        gradientConfiguration = GradientConfiguration.build();
        electionConfiguration = ElectionConfiguration.build();
        chunkManagerConfiguration = new ChunkManagerConfig(config);
        gradientConfig= new GradientConfig(config);
        electionConfig = new ElectionConfig(config);
        
        try {
            ip = InetAddress.getLocalHost();
            port = 9999;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
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
    public static SearchPeerInit generatePeerInit(DecoratedAddress simulatorAddress,Set<DecoratedAddress> bootstrap, long id){

        logger.info(" Generating address for peer with id: {} ", id);

        BasicAddress basicAddress = new BasicAddress(ip, port , (int)id);
        DecoratedAddress decoratedAddress = new DecoratedAddress(basicAddress);
        systemConfig= new SystemConfig(gradientConfiguration.getSeed() + id, decoratedAddress, simulatorAddress, new ArrayList<DecoratedAddress>(bootstrap));

        ApplicationSelf applicationSelf = new ApplicationSelf(decoratedAddress);
        SearchPeerInit init  = new SearchPeerInit(applicationSelf, systemConfig, croupierConfiguration, searchConfiguration, gradientConfiguration, electionConfiguration, chunkManagerConfiguration, gradientConfig, electionConfig);
        
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
    public static DecoratedAddress getNodeAddressToCommunicate(Long id){
        
        logger.info(" Fetching random node address from the map. ");
        Long successor = ringNodes.getNode(id);
        
        DecoratedAddress address = peersAddressMap.get(successor);
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
     * Generate the constant search pattern. 
     * For now generate a constant search pattern.
     *
     * <b>CAUTION:</b> Constant Search Pattern.
     * @return SearchPattern
     */
    public static SearchPattern generateSearchPattern(){
        SearchPattern searchPattern = new SearchPattern("sweep", 0, 0, null, null, null, MsConfig.Categories.Video, null);
        return searchPattern;
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
    
    
    public static DecoratedAddress getBasicAddress(long id){
        logger.info("C a l l e d .. .. .. " + id);
        return peersAddressMap.get(id);
    }
}
