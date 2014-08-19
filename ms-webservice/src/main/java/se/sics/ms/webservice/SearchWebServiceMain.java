/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.webservice;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.ports.UiPort;
import se.sics.ms.timeout.IndividualTimeout;

/**
 *
 * @author alidar
 */
public class SearchWebServiceMain extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(SearchWebServiceMain.class);
    Component network;
    Component timer;
    Component natTraverser;
    Component searchPeer;
    Component searchMiddleware;
    private Component resolveIp;
    private Self self;
    private Address myAddr;
    Positive<UiPort> uiPort = positive(UiPort.class);
    SearchDelegate delegate;
    private SearchWebServiceMain myComp;
    private String publicBootstrapNode = "cloud7.sics.se";

    public static class PsPortBindResponse extends PortBindResponse {
        public PsPortBindResponse(PortBindRequest request) {
            super(request);
        }
    }

    public SearchWebServiceMain(SearchWebServiceMiddlewareInit init) {

        myComp = this;
        subscribe(handleStart, control);
    }
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {

            resolveIp = create(ResolveIp.class, Init.NONE);
            timer = create(JavaTimer.class, Init.NONE);

            connect(resolveIp.getNegative(Timer.class), timer.getPositive(Timer.class));
            subscribe(handleGetIpResponse, resolveIp.getPositive(ResolveIpPort.class));

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

                self = new MsSelfImpl(ToVodAddr.systemAddr(myAddr));

                Set<Address> publicNodes = new HashSet<Address>();
                try {
                    InetAddress inet = InetAddress.getByName(publicBootstrapNode);
                    publicNodes.add(new Address(inet, MsConfig.getPort(), 0));
                } catch (UnknownHostException ex) {
                    java.util.logging.Logger.getLogger(SearchWebServiceMain.class.getName()).log(Level.SEVERE, null, ex);
                }

                natTraverser = create(NatTraverser.class, new NatTraverserInit(self, publicNodes, MsConfig.getSeed()));
                searchMiddleware = create(SearchWebServiceMiddleware.class, Init.NONE);
                try {
                    searchPeer = create(SearchPeer.class, new SearchPeerInit(self, CroupierConfiguration.build(), SearchConfiguration.build(), GradientConfiguration.build(), ElectionConfiguration.build(), ToVodAddr.bootstrap(new Address(InetAddress.getByName(publicBootstrapNode), MsConfig.getPort(), 0))));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }

                connect(natTraverser.getNegative(Timer.class), timer.getPositive(Timer.class));
                connect(natTraverser.getNegative(VodNetwork.class), network.getPositive(VodNetwork.class));
                connect(natTraverser.getNegative(NatNetworkControl.class), network.getPositive(NatNetworkControl.class));
                connect(searchMiddleware.getPositive(UiPort.class), searchPeer.getNegative(UiPort.class));
                connect(network.getPositive(VodNetwork.class), searchPeer.getNegative(VodNetwork.class), new MsgDestFilterAddress(myAddr));
                connect(timer.getPositive(Timer.class), searchPeer.getNegative(Timer.class),
                        new IndividualTimeout.IndividualTimeoutFilter(myAddr.getId()));

                subscribe(handleFault, natTraverser.getControl());

                trigger(Start.event, natTraverser.getControl());
                trigger(Start.event, searchMiddleware.getControl());
                trigger(Start.event, searchPeer.getControl());
            }
        }
    };
    public Handler<GetIpResponse> handleGetIpResponse = new Handler<GetIpResponse>() {
        @Override
        public void handle(GetIpResponse event) {

            int myId = (new Random(MsConfig.getSeed())).nextInt();
            InetAddress localIp = event.getIpAddress();
            myAddr = new Address(localIp, /*randInt(49152, 65535)*/MsConfig.getPort(), myId);

            network = create(NettyNetwork.class, new NettyInit(MsConfig.getSeed(), true, MessageFrameDecoder.class));

            subscribe(handleNettyFault, network.getControl());
            subscribe(handlePsPortBindResponse, network.getPositive(NatNetworkControl.class));

            trigger(Start.event, network.getControl());

            PortBindRequest pb1 = new PortBindRequest(myAddr, Transport.UDP);
            PsPortBindResponse pbr1 = new PsPortBindResponse(pb1);
            pb1.setResponse(pbr1);
            trigger(pb1, network.getPositive(NatNetworkControl.class));
        }
    };
    
    public static int randInt(int min, int max) {

        // Usually this should be a field rather than a method variable so
        // that it is not re-seeded every call.
        Random rand = new Random(MsConfig.getSeed());

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }
    
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
        int numWorkers = Math.max(1, cores - 2);

        System.setProperty("java.net.preferIPv4Stack", "true");
        try {
            // We use MsConfig in the NatTraverser component, so we have to 
            // initialize it.
            MsConfig.init(args);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(SearchWebServiceMain.class.getName()).log(Level.SEVERE, null, ex);
        }

        Kompics.createAndStart(SearchWebServiceMain.class, numWorkers);
    }
}
