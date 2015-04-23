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
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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
        electionConfig = new ElectionConfig.ElectionConfigBuilder(MsConfig.GRADIENT_VIEW_SIZE).buildElectionConfig();
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
    public static SearchPeerInit generatePeerInit(DecoratedAddress simulatorAddress, long id){
        
        logger.info(" Generating address for peer with id: {} ", id);
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        BasicAddress basicAddress = new BasicAddress(ip, 9999 , (int)id);
        ApplicationSelf applicationSelf = new ApplicationSelf(new DecoratedAddress(basicAddress));

        SearchPeerInit init  = new SearchPeerInit(applicationSelf, systemConfig, croupierConfiguration, searchConfiguration, gradientConfiguration, electionConfiguration, chunkManagerConfiguration, gradientConfig, electionConfig);
        
        ringNodes.addNode(id);
        peersAddressMap.put(id, applicationSelf.getAddress());

        List<DecoratedAddress> bootstrapNodes = new ArrayList<DecoratedAddress>();
        bootstrapNodes.add(applicationSelf.getAddress());
        systemConfig = new SystemConfig(null, simulatorAddress, bootstrapNodes);
        
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
