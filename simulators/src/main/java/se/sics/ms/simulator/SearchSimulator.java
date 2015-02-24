package se.sics.ms.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import se.sics.cm.ChunkManager;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.co.FailureDetectorComponent;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.*;
import se.sics.gvod.filters.MsgDestFilterAddress;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.Timer;
import se.sics.ipasdistances.AsIpGenerator;
import se.sics.kompics.*;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.SimulationEventsPort.AddIndexSimulated;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.simulation.*;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PartitionHelper;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

public final class SearchSimulator extends ComponentDefinition {

    Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    private final HashMap<Long, Component> peers;
    private final HashMap<Long, VodAddress> peersAddress;
    private CroupierConfig croupierConfiguration;
    private SearchConfiguration searchConfiguration;
    private GradientConfiguration gradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private ChunkManagerConfiguration chunkManagerConfiguration;
    private Long identifierSpaceSize;
    private ConsistentHashtable<Long> ringNodes;
    private AsIpGenerator ipGenerator;
    private MagnetFileIterator magnetFiles;
    static String[] articles = {" ", "The ", "QueryLimit "};
    static String[] verbs = {"fires ", "walks ", "talks ", "types ", "programs "};
    static String[] subjects = {"computer ", "Lucene ", "torrent "};
    static String[] objects = {"computer", "java", "video"};
    Random r = new Random(System.currentTimeMillis());
    int counter =0;
    Logger logger = LoggerFactory.getLogger(SearchSimulator.class);

    public SearchSimulator(SearchSimulatorInit init) {
        peers = new HashMap<Long, Component>();
        peersAddress = new HashMap<Long, VodAddress>();
        ringNodes = new ConsistentHashtable<Long>();

        peers.clear();

        croupierConfiguration = init.getCroupierConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        gradientConfiguration = init.getGradientConfiguration();
        electionConfiguration = init.getElectionConfiguration();
        chunkManagerConfiguration = init.getChunkManagerConfiguration();
        //TODO Alex/Croupier - what is this exactly? it was set to 3000 before
        identifierSpaceSize = new Long(3000);

        // TODO - is this the correct seed??!?
        ipGenerator = AsIpGenerator.getInstance(init.getGradientConfiguration().getSeed());
        
        subscribe(handleStart, control);
        subscribe(handleGenerateReport, timer);
        subscribe(handlePeerJoin, simulator);
        subscribe(handlePeerFail, simulator);
//        subscribe(handleTerminateExperiment, simulator);
        subscribe(handleAddIndexEntry, simulator);
        subscribe(handleAddMagnetEntry, simulator);
        subscribe(handleSearch, simulator);
    }

    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start init) {
            // generate periodic report
            int snapshotPeriod = Configuration.SNAPSHOT_PERIOD;
//            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod,
//                    snapshotPeriod);
//            spt.setTimeoutEvent(new GenerateReport(spt));
//            spt(spt, timer);

        }
    };

    String randomText() {
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

//    Handler<TerminateExperiment> handleTerminateExperiment = new Handler<TerminateExperiment>() {
//        @Override
//        public void handle(TerminateExperiment event) {
//            
//            // TODO: Print out summary
//            
//            Kompics.shutdown();
//            System.exit(0);
//        }
//    };

    Handler<AddIndexEntry> handleAddIndexEntry = new Handler<AddIndexEntry>() {
        @Override
        public void handle(AddIndexEntry event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);

            IndexEntry index = new IndexEntry("test url", randomText(), new Date(), MsConfig.Categories.Video, "", "Test Desc", "");
            index.setLeaderId(null);
            trigger(new AddIndexSimulated(index), peer.getNegative(SimulationEventsPort.class));
        }
    };

    /**
     * Add real magnet links from a specified xml file to the system.
     */
    Handler<AddMagnetEntry> handleAddMagnetEntry = new Handler<AddMagnetEntry>() {
        @Override
        public void handle(AddMagnetEntry event) {
            IndexEntry entry = null;
            try {
                // Lazy init because we don't need that in most cases
                if (magnetFiles == null) {
                    magnetFiles = new MagnetFileIterator("resources/poor3.xml");
                }
                entry = magnetFiles.next();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }

            // No more entries in the file
            if (entry == null) {
                return;
            }

            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            trigger(new AddIndexSimulated(entry), peer.getNegative(SimulationEventsPort.class));
        }
    };

    Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
        @Override
        public void handle(PeerJoin event) {
            
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

    Handler<PeerFail> handlePeerFail = new Handler<PeerFail>() {
        @Override
        public void handle(PeerFail event) {
            Long id = ringNodes.getNode(event.getId());

            if (ringNodes.size() == 0) {
                System.err.println("Empty network");
                return;
            }

            ringNodes.removeNode(id);
            stopAndDestroyPeer(id);
        }
    };

    Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
        @Override
        public void handle(GenerateReport event) {
            Snapshot.report();
        }
    };

    Handler<Search> handleSearch = new Handler<Search>() {
        @Override
        public void handle(Search event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            SearchPattern searchPattern = new SearchPattern("abhi", 0, 0, null, null, null, MsConfig.Categories.Video, null);
            trigger(new SimulationEventsPort.SearchSimulated(searchPattern), peer.getNegative(SimulationEventsPort.class));
        }
    };

    private VodAddress bootstrappingNode;
    private final void createAndStartNewPeer(long id) {

        System.out.println(" Going to start peer with id: " + id) ;
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        Address address = new Address(ip, 9999, (int) id);

        // FIXME: Changing the initial partitioning depth as zero.
        Self self = new MsSelfImpl(new VodAddress(address, 
                PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.NEVER_BEFORE, 0, 0, MsConfig.Categories.Video.ordinal())));

        Component peer = create(SearchPeer.class, new SearchPeerInit(self, croupierConfiguration, searchConfiguration,
                gradientConfiguration, electionConfiguration, chunkManagerConfiguration, bootstrappingNode, null));
        Component fd = create(FailureDetectorComponent.class, Init.NONE);
        connect(network, peer.getNegative(VodNetwork.class), new MsgDestFilterAddress(address));
        connect(timer, peer.getNegative(Timer.class), new IndividualTimeout.IndividualTimeoutFilter(self.getId()));
        connect(fd.getPositive(FailureDetectorPort.class), peer.getNegative(FailureDetectorPort.class));

        bootstrappingNode = self.getAddress();

        trigger(Start.event, peer.getControl());
        trigger(Start.event, fd.getControl());
        peers.put(id, peer);
        peersAddress.put(id, self.getAddress());

        Snapshot.addPeer(self.getAddress());
    }

    private void stopAndDestroyPeer(Long id) {
        Component peer = peers.get(id);

        trigger(new Stop(), peer.getControl());

        disconnect(network, peer.getNegative(VodNetwork.class));
        disconnect(timer, peer.getNegative(Timer.class));

        peers.remove(id);
        VodAddress addr = peersAddress.remove(id);
        Snapshot.removePeer(addr);

        destroy(peer);
    }
}
