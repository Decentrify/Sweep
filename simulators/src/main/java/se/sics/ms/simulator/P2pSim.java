package se.sics.ms.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.co.FailureDetectorComponent;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.*;
import se.sics.gvod.filters.MsgDestFilterAddress;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.ipasdistances.AsIpGenerator;
import se.sics.kompics.*;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.simulation.ConsistentHashtable;
import se.sics.ms.simulation.IndexEntryP2pSimulated;
import se.sics.ms.simulation.PeerJoinP2pSimulated;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by babbarshaer on 2015-02-13.
 */
public class P2pSim extends ComponentDefinition{

    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    private VodAddress self;
    private VodAddress simulatorAddress;
    private HashMap<Long, Component> peers;
    private HashMap<Long, VodAddress> peersAddress;
    private CroupierConfig croupierConfiguration;
    private SearchConfiguration searchConfiguration;
    private GradientConfiguration gradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private ChunkManagerConfiguration chunkManagerConfiguration;
    private Long identifierSpaceSize;
    private ConsistentHashtable<Long> ringNodes;
    private MagnetFileIterator magnetFiles;
    static String[] articles = {" ", "The ", "QueryLimit "};
    static String[] verbs = {"fires ", "walks ", "talks ", "types ", "programs "};
    static String[] subjects = {"computer ", "Lucene ", "torrent "};
    static String[] objects = {"computer", "java", "video"};
    Random r = new Random(System.currentTimeMillis());
    int counter =0;
    Logger logger = LoggerFactory.getLogger(P2pSim.class);
    private VodAddress bootstrappingNode;
    
    
    public P2pSim(P2pSimulatorInit init) throws IOException {
        
        // === Main Initialization.
        doInit(init);    

       // === Handlers Subscriptions.
        subscribe(startHandler,control);
        subscribe(handlePeerJoin, network);
        subscribe(handleAddIndexEntry,network);
    }

    private void doInit(P2pSimulatorInit init) {
        
        self = init.getSelf();
        simulatorAddress = init.getSimulatorAddress();
        peers = new HashMap<Long, Component>();
        peersAddress = new HashMap<Long,VodAddress>();
        ringNodes = new ConsistentHashtable<Long>();
        
        peers.clear();
        croupierConfiguration = init.getCroupierConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        gradientConfiguration = init.getGradientConfiguration();
        electionConfiguration = init.getElectionConfiguration();
        chunkManagerConfiguration = init.getChunkManagerConfiguration();
        identifierSpaceSize = new Long(3000);
    }
    
    
    Handler<Start> startHandler = new Handler<Start>(){
        @Override
        public void handle(Start event) {
            logger.info("Executing Start Event.");
        }
    };
    
    
    Handler<PeerJoinP2pSimulated.Request> handlePeerJoin = new Handler<PeerJoinP2pSimulated.Request>(){

        @Override
        public void handle(PeerJoinP2pSimulated.Request event) {

            logger.info("Received Peer Join: -> " + event.getPeerId());
            
            Long id = event.getPeerId();
            logger.info(" Handle Peer Join Received for id -> " + id);
            // join with the next id if this id is taken
            Long successor = ringNodes.getNode(id);

            while (successor != null && successor.equals(id)) {
                id = (id + 1) % identifierSpaceSize;
                successor = ringNodes.getNode(id);
            }

            createAndStartNewPeer(id);
            ringNodes.addNode(id);
                
        }

    };


    Handler<IndexEntryP2pSimulated.Request> handleAddIndexEntry = new Handler<IndexEntryP2pSimulated.Request>() {
        @Override
        public void handle(IndexEntryP2pSimulated.Request event) {
            
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);

            IndexEntry index = new IndexEntry("test url", randomText(), new Date(), MsConfig.Categories.Video, "", "Test Desc", "");
            index.setLeaderId(null);
            trigger(new SimulationEventsPort.AddIndexSimulated(index), peer.getNegative(SimulationEventsPort.class));
        }
    };


    
    /**
     * Boot up new instance of a peer.
     * @param id
     */
    private void createAndStartNewPeer(long id) {

        System.out.println(" Going to start peer with id: " + id) ;
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // Constructing address of the peer.
        Address address = new Address(ip, 9999, (int) id);
        Self self = new MsSelfImpl(new VodAddress(address,
                PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.NEVER_BEFORE, 0, 0, MsConfig.Categories.Video.ordinal())));

        // Making connections.
        Component peer = create(SearchPeer.class, new SearchPeerInit(self, croupierConfiguration, searchConfiguration,
                gradientConfiguration, electionConfiguration, chunkManagerConfiguration, bootstrappingNode, simulatorAddress));
        Component fd = create(FailureDetectorComponent.class, Init.NONE);
        connect(network, peer.getNegative(VodNetwork.class), new MsgDestFilterAddress(address));
        connect(timer, peer.getNegative(Timer.class), new IndividualTimeout.IndividualTimeoutFilter(self.getId()));
        connect(fd.getPositive(FailureDetectorPort.class), peer.getNegative(FailureDetectorPort.class));

        bootstrappingNode = self.getAddress();

        // Booting up the peer.
        trigger(Start.event, peer.getControl());
        trigger(Start.event, fd.getControl());
        peers.put(id, peer);
        peersAddress.put(id, self.getAddress());

        // Informing snapshot about the update.
        Snapshot.addPeer(self.getAddress());
    }


    /**
     * Construct a random text as index entry name.
     * @return
     */
    private String randomText() {
        StringBuilder sb = new StringBuilder();
//        int clauses = Math.max(1, r.nextInt(3));
//        for (int i = 0; i < clauses; i++) {
//            sb.append(articles[r.nextInt(articles.length)]);
//            sb.append(subjects[r.nextInt(subjects.length)]);
//            sb.append(verbs[r.nextInt(verbs.length)]);
//            sb.append(objects[r.nextInt(objects.length)]);
//            sb.append(". ");
//        }
        sb.append("abhi"+counter);
        counter++;
        return sb.toString();
    }
    
    
}
