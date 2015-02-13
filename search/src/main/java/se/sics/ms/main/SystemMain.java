/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.main;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.co.FailureDetectorComponent;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.util.ToVodAddr;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.filters.MsgDestFilterAddress;
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
import se.sics.kompics.*;
import se.sics.kompics.nat.utils.getip.ResolveIp;
import se.sics.kompics.nat.utils.getip.ResolveIpPort;
import se.sics.kompics.nat.utils.getip.events.GetIpRequest;
import se.sics.kompics.nat.utils.getip.events.GetIpResponse;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.croupier.CroupierEncodeDecode;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.ports.UiPort;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.p2ptoolbox.croupier.api.CroupierSelectionPolicy;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

/**
 *
 * @author alidar
 */
public class SystemMain extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(SystemMain.class);
    Component network;
    Component timer;
    Component natTraverser;
    Component searchPeer;
    private Component resolveIp;
    private Self self;
    private Address myAddr;
    private SystemMain myComp;
//    private String publicBootstrapNode = "cloud7.sics.se";
    private Address bootstrapAddress;
    private int bindCount = 0; //

    public static class PsPortBindResponse extends PortBindResponse {

        public PsPortBindResponse(PortBindRequest request) {
            super(request);
        }
    }

    public SystemMain() {

        myComp = this;
        subscribe(handleStart, control);
        
        CroupierEncodeDecode.init();
        
        resolveIp = create(ResolveIp.class, Init.NONE);
        timer = create(JavaTimer.class, Init.NONE);
        bootstrapAddress = MsConfig.getBootstrapServer();
        connect(resolveIp.getNegative(Timer.class), timer.getPositive(Timer.class));
        subscribe(handleGetIpResponse, resolveIp.getPositive(ResolveIpPort.class));

    }
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new GetIpRequest(false),
                    resolveIp.getPositive(ResolveIpPort.class));
        }
    };
    private Handler<PsPortBindResponse> handlePsPortBindResponse
            = new Handler<PsPortBindResponse>() {
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

                        bindCount++;
                        if (bindCount == 2) { //if both UDP and TCP ports have successfully binded.
                            self = new MsSelfImpl(ToVodAddr.systemAddr(myAddr));

                            Set<Address> publicNodes = new HashSet<Address>();
                            try {
//                                InetAddress inet = InetAddress.getByName(publicBootstrapNode);
                                if (bootstrapAddress != null) {
                                    publicNodes.add(bootstrapAddress);
                                } else {
                                    throw new UnknownHostException("Bootstrap address unknown.");
                                }

                            } catch (UnknownHostException ex) {
                                java.util.logging.Logger.getLogger(SystemMain.class.getName()).log(Level.SEVERE, null, ex);
                            }

                            natTraverser = create(NatTraverser.class, new NatTraverserInit(self, publicNodes, MsConfig.getSeed()));

                            //TODO Alex/Croupier get croupier selection policy from settings
                            CroupierSelectionPolicy hardcodedPolicy = CroupierSelectionPolicy.RANDOM;
                            CroupierConfig croupierConfig = new CroupierConfig(MsConfig.CROUPIER_VIEW_SIZE, MsConfig.CROUPIER_SHUFFLE_PERIOD,
                                    MsConfig.CROUPIER_SHUFFLE_PERIOD, hardcodedPolicy);
                            searchPeer = create(SearchPeer.class, new SearchPeerInit(self, croupierConfig,
                                            SearchConfiguration.build(), GradientConfiguration.build(),
                                            ElectionConfiguration.build(), ChunkManagerConfiguration.build(),
                                            ToVodAddr.bootstrap(bootstrapAddress)));

                            Component fd = create(FailureDetectorComponent.class, Init.NONE);

                            connect(natTraverser.getNegative(Timer.class), timer.getPositive(Timer.class));
                            connect(natTraverser.getNegative(VodNetwork.class), network.getPositive(VodNetwork.class));
                            connect(natTraverser.getNegative(NatNetworkControl.class), network.getPositive(NatNetworkControl.class));

                            /**
                             * Filter not working for some reason so commenting
                             * it.*
                             */
//                            connect(network.getPositive(VodNetwork.class), searchPeer.getNegative(VodNetwork.class),new MsgDestFilterAddress(myAddr));
                            connect(network.getPositive(VodNetwork.class), searchPeer.getNegative(VodNetwork.class));
                            connect(timer.getPositive(Timer.class), searchPeer.getNegative(Timer.class),
                                    new IndividualTimeout.IndividualTimeoutFilter(myAddr.getId()));
                            connect(fd.getPositive(FailureDetectorPort.class), searchPeer.getNegative(FailureDetectorPort.class));

                            subscribe(handleNettyFault, natTraverser.getControl());

                            trigger(Start.event, natTraverser.getControl());
                            trigger(Start.event, searchPeer.getControl());
                            trigger(Start.event, fd.getControl());

                        }
                    }
                }
            };
    public Handler<GetIpResponse> handleGetIpResponse = new Handler<GetIpResponse>() {
        @Override
        public void handle(GetIpResponse event) {

            //FIXME: Fix the case in which the different nodes in which the initial seed is generated same.
//            int myId = (new Random(MsConfig.getSeed())).nextInt();
            int myId = (new Random()).nextInt();

            InetAddress localIp = event.getIpAddress();

            logger.info("My Local Ip Address returned from ResolveIp is:  " + localIp.getHostName());
            if (localIp.getHostName().equals(bootstrapAddress.getIp().getHostName())) {
                myId = 0;
            }

            // Bind Udt and Udp on separate ports in the system for now.
            myAddr = new Address(localIp, MsConfig.getPort(), myId);
            Address myUdtAddr = new Address(localIp, myAddr.getPort() + 1, myId);

            network = create(NettyNetwork.class, new NettyInit(MsConfig.getSeed(), true, MessageFrameDecoder.class));

            subscribe(handleNettyFault, network.getControl());
            subscribe(handlePsPortBindResponse, network.getPositive(NatNetworkControl.class));

            trigger(Start.event, network.getControl());

            bindPort(Transport.UDP, myAddr);
            bindPort(Transport.UDT, myUdtAddr);
        }
    };

    void bindPort(Transport transport, Address address) {

        logger.warn("Sending a port bind request for : " + address.toString());
        PortBindRequest udpPortBindReq = new PortBindRequest(address, transport);
        PsPortBindResponse pbr1 = new PsPortBindResponse(udpPortBindReq);
        udpPortBindReq.setResponse(pbr1);
        trigger(udpPortBindReq, network.getPositive(NatNetworkControl.class));

    }

    public static int randInt(int min, int max) {

        // Usually this should be a field rather than a method variable so
        // that it is not re-seeded every call.
        Random rand = new Random(MsConfig.getSeed());

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

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
            // Initialize the MsConfig Component.
            MsConfig.init(args);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        Kompics.createAndStart(SystemMain.class, numWorkers);
    }
}
