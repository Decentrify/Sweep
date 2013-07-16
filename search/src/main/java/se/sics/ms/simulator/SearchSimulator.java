package se.sics.ms.simulator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.Timer;
import se.sics.ipasdistances.AsIpGenerator;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import se.sics.kompics.web.WebResponse;
import se.sics.gvod.config.Configuration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.peersearch.types.IndexEntry;
import se.sics.ms.peer.IndexPort;
import se.sics.ms.peer.IndexPort.AddIndexSimulated;
import se.sics.ms.peer.SearchPeer;
import se.sics.ms.peer.SearchPeerInit;
import se.sics.ms.simulation.AddIndexEntry;
import se.sics.ms.simulation.AddMagnetEntry;
import se.sics.ms.simulation.ConsistentHashtable;
import se.sics.ms.simulation.GenerateReport;
import se.sics.ms.simulation.PeerFail;
import se.sics.ms.simulation.PeerJoin;
import se.sics.ms.snapshot.Snapshot;

public final class SearchSimulator extends ComponentDefinition {

    Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<Web> webIncoming = negative(Web.class);
    private final HashMap<Long, Component> peers;
    private final HashMap<Long, Address> peersAddress;
    private CroupierConfiguration croupierConfiguration;
    private SearchConfiguration searchConfiguration;
    private GradientConfiguration gradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private Long identifierSpaceSize;
    private ConsistentHashtable<Long> ringNodes;
    private AsIpGenerator ipGenerator = AsIpGenerator.getInstance(125);
    private MagnetFileIterator magnetFiles;
    static String[] articles = {" ", "The ", "A "};
    static String[] verbs = {"fires ", "walks ", "talks ", "types ", "programs "};
    static String[] subjects = {"computer ", "Lucene ", "torrent "};
    static String[] objects = {"computer", "java", "video"};
    Random r = new Random(System.currentTimeMillis());

    public SearchSimulator() {
        peers = new HashMap<Long, Component>();
        peersAddress = new HashMap<Long, Address>();
        ringNodes = new ConsistentHashtable<Long>();

        subscribe(handleInit, control);
        subscribe(handleGenerateReport, timer);
        subscribe(handlePeerJoin, simulator);
        subscribe(handlePeerFail, simulator);
        subscribe(handleAddIndexEntry, simulator);
        subscribe(handleAddMagnetEntry, simulator);
        subscribe(handleWebRequest, webIncoming);
    }
    Handler<SimulatorInit> handleInit = new Handler<SimulatorInit>() {
        @Override
        public void handle(SimulatorInit init) {
            peers.clear();

            croupierConfiguration = init.getCroupierConfiguration();
            searchConfiguration = init.getSearchConfiguration();
            gradientConfiguration = init.getGradientConfiguration();
            electionConfiguration = init.getElectionConfiguration();

            identifierSpaceSize = croupierConfiguration.getRto();

            // generate periodic report
            int snapshotPeriod = Configuration.SNAPSHOT_PERIOD;
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod,
                    snapshotPeriod);
            spt.setTimeoutEvent(new GenerateReport(spt));
            trigger(spt, timer);

        }
    };

    String randomText() {
        StringBuilder sb = new StringBuilder();
        int clauses = Math.max(1, r.nextInt(3));
        for (int i = 0; i < clauses; i++) {
            sb.append(articles[r.nextInt(articles.length)]);
            sb.append(subjects[r.nextInt(subjects.length)]);
            sb.append(verbs[r.nextInt(verbs.length)]);
            sb.append(objects[r.nextInt(objects.length)]);
            sb.append(". ");
        }
        return sb.toString();
    }
    Handler<WebRequest> handleWebRequest = new Handler<WebRequest>() {
        @Override
        public void handle(WebRequest event) {
            // Find closest peer and send web request on to it.
            long peerId = ringNodes.getNode((long) event.getDestination());
            Component peer = peers.get(peerId);
            trigger(event, peer.getPositive(Web.class));
        }
    };
    Handler<WebResponse> handleWebResponse = new Handler<WebResponse>() {
        @Override
        public void handle(WebResponse event) {
            trigger(event, webIncoming);
        }
    };
    Handler<AddIndexEntry> handleAddIndexEntry = new Handler<AddIndexEntry>() {
        @Override
        public void handle(AddIndexEntry event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);

            IndexEntry index = new IndexEntry("", "", IndexEntry.Category.Books, "", "");
            index.setFileName(randomText());
            index.setLeaderId("");
            trigger(new AddIndexSimulated(index), peer.getNegative(IndexPort.class));
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
            trigger(new AddIndexSimulated(entry), peer.getNegative(IndexPort.class));
        }
    };
    Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
        @Override
        public void handle(PeerJoin event) {
            Long id = event.getPeerId();

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

    private final void createAndStartNewPeer(long id) {
        int overlayId = 1;
        Component peer = create(SearchPeer.class);
        InetAddress ip = null;
            try {
                ip = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        Address address = new Address(ip, 9999, (int) id);

        Self self = new SelfImpl(new VodAddress(address, overlayId));

        //connect(network, peer.getNegative(VodNetwork.class), new MsgDestFilterAddress(address));
        connect(network, peer.getNegative(VodNetwork.class));
        connect(timer, peer.getNegative(Timer.class));
        subscribe(handleWebResponse, peer.getPositive(Web.class));

        trigger(new SearchPeerInit(self, croupierConfiguration,
                searchConfiguration, gradientConfiguration, electionConfiguration), peer.getControl());

        trigger(new Start(), peer.getControl());
        peers.put(id, peer);
        peersAddress.put(id, address);

        Snapshot.addPeer(new VodAddress(address, overlayId));
    }

    private void stopAndDestroyPeer(Long id) {
        Component peer = peers.get(id);

        trigger(new Stop(), peer.getControl());

        disconnect(network, peer.getNegative(VodNetwork.class));
        disconnect(timer, peer.getNegative(Timer.class));

        peers.remove(id);
        Address addr = peersAddress.remove(id);
        Snapshot.removePeer(addr);

        destroy(peer);
    }
}
