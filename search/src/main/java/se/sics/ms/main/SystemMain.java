package se.sics.ms.main;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.common.util.ToVodAddr;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.nat.traversal.NatTraverser;
import se.sics.gvod.nat.traversal.events.NatTraverserInit;
import se.sics.gvod.net.NatNetworkControl;
import se.sics.gvod.net.NettyInit;
import se.sics.gvod.net.NettyNetwork;
import se.sics.gvod.net.Transport;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.net.events.PortBindRequest;
import se.sics.gvod.net.events.PortBindResponse;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.java.JavaTimer;
import se.sics.kompics.Component;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Start;
import se.sics.kompics.nat.utils.getip.ResolveIp;
import se.sics.kompics.nat.utils.getip.ResolveIpPort;
import se.sics.kompics.nat.utils.getip.events.GetIpRequest;
import se.sics.kompics.nat.utils.getip.events.GetIpResponse;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.peer.SearchPeerInit;
import se.sics.ms.peer.SearchUiPort;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.ms.peer.SearchPeer;

public class SystemMain extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(SystemMain.class);
    Component network;
    Component timer;
    Component natTraverser;
    Component searchPeer;
    Component ui;
    private Component resolveIp;
    private Self self;
    private Address myAddr;
    private String publicBootstrapNode = "cloud7.sics.se";
    
    public static class PsPortBindResponse extends PortBindResponse {
        public PsPortBindResponse(PortBindRequest request) {
            super(request);
        }
    }

    public SystemMain() {
        network = create(NettyNetwork.class);
        timer = create(JavaTimer.class);
        natTraverser = create(NatTraverser.class);
        searchPeer = create(SearchPeer.class);
        ui = create(UiComponent.class);

        resolveIp = create(ResolveIp.class);

        connect(natTraverser.getNegative(Timer.class), timer.getPositive(Timer.class));
        connect(natTraverser.getNegative(VodNetwork.class), network.getPositive(VodNetwork.class));
        connect(natTraverser.getNegative(NatNetworkControl.class), network.getPositive(NatNetworkControl.class));
        connect(resolveIp.getNegative(Timer.class), timer.getPositive(Timer.class));
        connect(ui.getNegative(Timer.class), timer.getPositive(Timer.class));

        connect(searchPeer.getPositive(SearchUiPort.class), ui.getNegative(SearchUiPort.class));

        subscribe(handleStart, control);
        subscribe(handleGetIpResponse, resolveIp.getPositive(ResolveIpPort.class));
        subscribe(handleFault, natTraverser.getControl());
        subscribe(handleNettyFault, network.getControl());
        subscribe(handlePsPortBindResponse, network.getPositive(NatNetworkControl.class));
    }
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {

            trigger(new GetIpRequest(false),
                    resolveIp.getPositive(ResolveIpPort.class));

        }
    };
    private Handler<PsPortBindResponse> handlePsPortBindResponse =
            new Handler<PsPortBindResponse>() {
        @Override
        public void handle(PsPortBindResponse event) {
            
            if (event.getStatus() != PortBindResponse.Status.SUCCESS) {
                logger.warn("Couldn't bind to port {}. Either another instance of the program is"
                        + "already running, or that port is being used by a different program. Go"
                        + "to settings to change the port in use. Status: ", event.getPort(),
                        event.getStatus());
                Kompics.shutdown();
                System.exit(-1);
            } else {
            
            self = new SelfImpl(ToVodAddr.systemAddr(myAddr));

            Set<Address> publicNodes = new HashSet<Address>();
            try {
                InetAddress inet = InetAddress.getByName(publicBootstrapNode);
                publicNodes.add(new Address(inet, MsConfig.getPort(), 0));
            } catch (UnknownHostException ex) {
                java.util.logging.Logger.getLogger(SystemMain.class.getName()).log(Level.SEVERE, null, ex);
            }

            trigger(new NatTraverserInit(self, publicNodes, MsConfig.getSeed()),
                    natTraverser.getControl());

            trigger(new SearchPeerInit(self, CroupierConfiguration.build(), SearchConfiguration.build(), GradientConfiguration.build(), ElectionConfiguration.build()),
                    searchPeer.getControl());
            }

            trigger(new UiComponentInit(self), ui.getControl());
            
        }
    };
    public Handler<GetIpResponse> handleGetIpResponse = new Handler<GetIpResponse>() {
        @Override
        public void handle(GetIpResponse event) {

            // TODO - how to get my id.
            int myId = 1123;
            InetAddress localIp = event.getIpAddress();
            myAddr = new Address(localIp, MsConfig.getPort(), myId);
            NettyInit nInit = new NettyInit(MsConfig.getSeed(), true, MessageFrameDecoder.class);
            trigger(nInit, network.getControl());

            PortBindRequest pb1 = new PortBindRequest(myAddr, Transport.UDP);
            PsPortBindResponse pbr1 = new PsPortBindResponse(pb1);
            pb1.setResponse(pbr1);
            trigger(pb1, network.getPositive(NatNetworkControl.class));

        }
    };
    public Handler<Fault> handleFault =
            new Handler<Fault>() {
        @Override
        public void handle(Fault ex) {

            logger.debug(ex.getFault().toString());
            System.exit(-1);
        }
    };
    Handler<Fault> handleNettyFault = new Handler<Fault>() {
        @Override
        public void handle(Fault msg) {
            logger.error("Problem in Netty: {}", msg.getFault().getMessage());
            System.exit(-1);
        }
    };

    /**
     * Starts the execution of the program
     *
     * @param args the command line arguments
     * @throws IOException in case the configuration file couldn't be created
     */
    public static void main(String[] args) throws IOException {

        int cores = Runtime.getRuntime().availableProcessors();
        int numWorkers = Math.max(1, cores - 1);

        System.setProperty("java.net.preferIPv4Stack", "true");
        try {
            // We use MsConfig in the NatTraverser component, so we have to 
            // initialize it.
            MsConfig.init(args);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(SystemMain.class.getName()).log(Level.SEVERE, null, ex);
        }

        Kompics.createAndStart(SystemMain.class, numWorkers);
    }
}
