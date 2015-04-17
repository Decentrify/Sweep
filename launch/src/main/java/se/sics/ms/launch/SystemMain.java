///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//package se.sics.ms.launch;
//
//import org.apache.commons.cli.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.cm.ChunkManagerConfiguration;
//import se.sics.co.FailureDetectorComponent;
//import se.sics.co.FailureDetectorPort;
//import se.sics.gvod.address.Address;
//import se.sics.gvod.common.Self;
//import se.sics.gvod.common.util.ToVodAddr;
//import se.sics.gvod.config.ElectionConfiguration;
//import se.sics.gvod.config.GradientConfiguration;
//import se.sics.gvod.config.SearchConfiguration;
//import se.sics.gvod.nat.traversal.NatTraverser;
//import se.sics.gvod.nat.traversal.events.NatTraverserInit;
//import se.sics.gvod.net.*;
//import se.sics.gvod.net.events.PortBindRequest;
//import se.sics.gvod.net.events.PortBindResponse;
//import se.sics.gvod.timer.Timer;
//import se.sics.gvod.timer.java.JavaTimer;
//import se.sics.kompics.*;
//import se.sics.kompics.nat.utils.getip.ResolveIp;
//import se.sics.kompics.nat.utils.getip.ResolveIpPort;
//import se.sics.kompics.nat.utils.getip.events.GetIpRequest;
//import se.sics.kompics.nat.utils.getip.events.GetIpResponse;
//import se.sics.ms.common.ApplicationSelf;
//import se.sics.ms.common.MsSelfImpl;
//import se.sics.ms.configuration.MsConfig;
//import se.sics.ms.net.MessageFrameDecoder;
//import se.sics.ms.search.SearchPeer;
//import se.sics.ms.search.SearchPeerInit;
//import se.sics.ms.timeout.IndividualTimeout;
//import se.sics.p2ptoolbox.croupier.api.CroupierSelectionPolicy;
//import se.sics.p2ptoolbox.croupier.core.CroupierConfig;
//import se.sics.p2ptoolbox.election.core.ElectionConfig;
//import se.sics.p2ptoolbox.gradient.core.GradientConfig;
//import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
//import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
//
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.*;
//import java.util.logging.Level;
//
///**
// *
// * @author alidar
// */
//public class SystemMain extends ComponentDefinition {
//
//    private static final Logger logger = LoggerFactory.getLogger(SystemMain.class);
//    Component network;
//    Component timer;
//    Component natTraverser;
//    Component searchPeer;
//    private Component resolveIp;
//    private Self self;
//    private ApplicationSelf applicationSelf;
//    private BasicAddress myAddr;
//    private SystemMain myComp;
//    private BasicAddress bootstrapAddress;
//    private int bindCount = 0; //
//
//    // Create Options for Command Line Parsing.
//    private Options options = new Options();
//    private CommandLine line;
//    private CommandLineParser parser;
//
//    private static String[] arguments;
//
//
//    // Aggregator Component Information.
//    private String aggregatorIp = "cloud7.sics.se";
//    private int aggregatorId = 0;
//    private int aggregatorPort = 54321;
//    private BasicAddress aggregatorAddress;
//
//
//    public static class PsPortBindResponse extends PortBindResponse {
//
//        public PsPortBindResponse(PortBindRequest request) {
//            super(request);
//        }
//    }
//
//    public SystemMain() {
//
//        init();
//        subscribe(handleStart, control);
//
//        resolveIp = create(ResolveIp.class, Init.NONE);
//        timer = create(JavaTimer.class, Init.NONE);
//        bootstrapAddress = null;
//        connect(resolveIp.getNegative(Timer.class), timer.getPositive(Timer.class));
//        subscribe(handleGetIpResponse, resolveIp.getPositive(ResolveIpPort.class));
//
//    }
//
//
//
//    public void init(){
//
//        myComp = this;
//        CommonEncodeDecode.init();
//
//        List<String> argList = new ArrayList<String>();
//        for (int i = 0; i < arguments.length; i++) {
//            if (arguments[i].startsWith("-X")) {
//                argList.add(arguments[i]);
//            }
//        }
//
//        Option aggregatorIpOption = new Option("XaIp", true, "Aggregator Ip Address");
//        Option aggregatorIdOption = new Option("XaId", true, "Aggregator Component Id");
//        Option aggregatorPortOption = new Option("XaPort", true, "Aggregator Port");
//
//        options.addOption(aggregatorIpOption);
//        options.addOption(aggregatorIdOption);
//        options.addOption(aggregatorPortOption);
//
//        parser = new GnuParser();
//
//        try{
//            line = parser.parse(options,argList.toArray(new String[argList.size()]));
//
//        } catch (ParseException e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//
//        if(line.hasOption(aggregatorIpOption.getOpt())){
//            aggregatorIp = line.getOptionValue(aggregatorIpOption.getOpt());
//            logger.warn(" Aggregator Ip Option Set: {}", aggregatorIp);
//        }
//
//        if(line.hasOption(aggregatorIdOption.getOpt())){
//            aggregatorId = Integer.parseInt(line.getOptionValue(aggregatorIdOption.getOpt()));
//            logger.warn(" Aggregator Id Option Set: {}", aggregatorId);
//        }
//
//        if(line.hasOption(aggregatorPortOption.getOpt())){
//            aggregatorPort = Integer.parseInt(line.getOptionValue(aggregatorPortOption.getOpt()));
//            logger.warn(" Aggregator Port Option Set: {}", aggregatorPort);
//        }
//
//    }
//
//    Handler<Start> handleStart = new Handler<Start>() {
//        @Override
//        public void handle(Start event) {
//            trigger(new GetIpRequest(false),
//                    resolveIp.getPositive(ResolveIpPort.class));
//        }
//    };
//    private Handler<PsPortBindResponse> handlePsPortBindResponse
//            = new Handler<PsPortBindResponse>() {
//                @Override
//                public void handle(PsPortBindResponse event) {
//
//                    if (event.getStatus() != PortBindResponse.Status.SUCCESS) {
//                        logger.warn("Couldn't bind to port {}. Either another instance of the program is"
//                                + "already running, or that port is being used by a different program. Go"
//                                + "to settings to change the port in use. Status: ", event.getPort(),
//                                event.getStatus());
//                        Kompics.shutdown();
//                        System.exit(-1);
//                    } else {
//
//                        bindCount++;
//                        if (bindCount == 2) { //if both UDP and TCP ports have successfully binded.
////                            self = new MsSelfImpl(ToVodAddr.systemAddr(myAddr));
//
//                            BasicAddress basicAddress = new BasicAddress(myAddr.getIp(), myAddr.getPort(), myAddr.getId());
//                            applicationSelf = new ApplicationSelf(new DecoratedAddress(basicAddress));
//
//                            Set<Address> publicNodes = new HashSet<Address>();
//                            try {
////                                InetAddress inet = InetAddress.getByName(publicBootstrapNode);
//                                if (bootstrapAddress != null) {
//                                    publicNodes.add(bootstrapAddress);
//                                } else {
//                                    throw new UnknownHostException("Bootstrap address unknown.");
//                                }
//
//                            } catch (UnknownHostException ex) {
//                                java.util.logging.Logger.getLogger(SystemMain.class.getName()).log(Level.SEVERE, null, ex);
//                            }
//
//                            natTraverser = create(NatTraverser.class, new NatTraverserInit(self, publicNodes, MsConfig.getSeed()));
//
//                            //TODO Alex/Croupier get croupier selection policy from settings.
//
//                            CroupierSelectionPolicy croupierPolicy;
//
//                            switch(MsConfig.CROUPIER_SELECTION_POLICY) {
//
//                                case HEALER: croupierPolicy = CroupierSelectionPolicy.HEALER;
//                                    break;
//
//                                case RANDOM: croupierPolicy = CroupierSelectionPolicy.RANDOM;
//                                    break;
//
//                                case TAIL: croupierPolicy = CroupierSelectionPolicy.TAIL;
//                                    break;
//
//                                default :
//                                    croupierPolicy = CroupierSelectionPolicy.HEALER;
//                            }
//
//                            CroupierConfig croupierConfig = new CroupierConfig(MsConfig.CROUPIER_VIEW_SIZE, MsConfig.CROUPIER_SHUFFLE_PERIOD,
//                                    MsConfig.CROUPIER_SHUFFLE_LENGTH, croupierPolicy);
//
//                            GradientConfig gradientConfig = new GradientConfig(MsConfig.GRADIENT_VIEW_SIZE,MsConfig.GRADIENT_SHUFFLE_PERIOD, MsConfig.GRADIENT_SHUFFLE_LENGTH);
//
//                            ElectionConfig electionConfig = new ElectionConfig.ElectionConfigBuilder(MsConfig.GRADIENT_VIEW_SIZE).buildElectionConfig();
//
//                            aggregatorAddress = getAggregatorAddress();
//
//                            searchPeer = create(SearchPeer.class, new SearchPeerInit(applicationSelf, croupierConfig,
//                                            SearchConfiguration.build(), GradientConfiguration.build(),
//                                            ElectionConfiguration.build(), ChunkManagerConfiguration.build(), gradientConfig,
//                                            ToVodAddr.bootstrap(bootstrapAddress),  (aggregatorAddress!= null) ? new DecoratedAddress(aggregatorAddress) : null, electionConfig ));
//
//                            Component fd = create(FailureDetectorComponent.class, Init.NONE);
//
//                            connect(natTraverser.getNegative(Timer.class), timer.getPositive(Timer.class));
//                            connect(natTraverser.getNegative(VodNetwork.class), network.getPositive(VodNetwork.class));
//                            connect(natTraverser.getNegative(NatNetworkControl.class), network.getPositive(NatNetworkControl.class));
//
//                            /**
//                             * Filter not working for some reason so commenting
//                             * it.*
//                             */
////                            connect(network.getPositive(VodNetwork.class), searchPeer.getNegative(VodNetwork.class),new MsgDestFilterAddress(myAddr));
//                            connect(network.getPositive(VodNetwork.class), searchPeer.getNegative(VodNetwork.class));
//                            connect(timer.getPositive(Timer.class), searchPeer.getNegative(Timer.class),
//                                    new IndividualTimeout.IndividualTimeoutFilter(myAddr.getId()));
//                            connect(fd.getPositive(FailureDetectorPort.class), searchPeer.getNegative(FailureDetectorPort.class));
//
//                            subscribe(handleNettyFault, natTraverser.getControl());
//
//                            trigger(Start.event, natTraverser.getControl());
//                            trigger(Start.event, searchPeer.getControl());
//                            trigger(Start.event, fd.getControl());
//
//                        }
//                    }
//                }
//            };
//
//    /**
//     * Based on the parameters passed, construct the aggregator address.
//     * @return Aggregator Address.
//     */
//    private BasicAddress getAggregatorAddress(){
//
//        BasicAddress aggregatorAddress = null;
//        try {
//
//            InetAddress ipAddr = InetAddress.getByName(aggregatorIp);
//            aggregatorAddress = new BasicAddress(ipAddr, aggregatorPort, aggregatorId);
//
//        }
//        catch (UnknownHostException e) {
//            e.printStackTrace();
//            return null;
//        }
//
//        return aggregatorAddress;
//    }
//
//    public Handler<GetIpResponse> handleGetIpResponse = new Handler<GetIpResponse>() {
//        @Override
//        public void handle(GetIpResponse event) {
//
//            //FIXME: Fix the case in which the different nodes in which the initial seed is generated same.
////            int myId = (new Random(MsConfig.getSeed())).nextInt();
//            int myId = (new Random()).nextInt();
//
//            InetAddress localIp = event.getIpAddress();
//
//            logger.info("My Local Ip Address returned from ResolveIp is:  " + localIp.getHostName());
//            if (localIp.getHostName().equals(bootstrapAddress.getIp().getHostName())) {
//                myId = 0;
//            }
//
//            // Bind Udt and Udp on separate ports in the system for now.
//            myAddr = new Address(localIp, MsConfig.getPort(), myId);
//            Address myUdtAddr = new Address(localIp, myAddr.getPort() + 1, myId);
//
//            network = create(NettyNetwork.class, new NettyInit(MsConfig.getSeed(), true, MessageFrameDecoder.class));
//
//            subscribe(handleNettyFault, network.getControl());
//            subscribe(handlePsPortBindResponse, network.getPositive(NatNetworkControl.class));
//
//            trigger(Start.event, network.getControl());
//
//            bindPort(Transport.UDP, myAddr);
//            bindPort(Transport.UDT, myUdtAddr);
//        }
//    };
//
//    void bindPort(Transport transport, Address address) {
//
//        logger.warn("Sending a port bind request for : " + address.toString());
//        PortBindRequest udpPortBindReq = new PortBindRequest(address, transport);
//        PsPortBindResponse pbr1 = new PsPortBindResponse(udpPortBindReq);
//        udpPortBindReq.setResponse(pbr1);
//        trigger(udpPortBindReq, network.getPositive(NatNetworkControl.class));
//
//    }
//
//    public static int randInt(int min, int max) {
//
//        // Usually this should be a field rather than a method variable so
//        // that it is not re-seeded every call.
//        Random rand = new Random(MsConfig.getSeed());
//
//        // nextInt is normally exclusive of the top value,
//        // so add 1 to make it inclusive
//        int randomNum = rand.nextInt((max - min) + 1) + min;
//
//        return randomNum;
//    }
//
//    Handler<Fault> handleNettyFault = new Handler<Fault>() {
//        @Override
//        public void handle(Fault msg) {
//            logger.error("Problem in Netty: {}", msg.getFault().getMessage());
//            System.exit(-1);
//        }
//    };
//
//    /**
//     * Starts the execution of the program
//     *
//     * @param args the command line arguments
//     * @throws java.io.IOException in case the configuration file couldn't be created
//     */
//    public static void main(String[] args) throws IOException {
//
//        int cores = Runtime.getRuntime().availableProcessors();
//        int numWorkers = Math.max(1, cores - 1);
//
//        System.setProperty("java.net.preferIPv4Stack", "true");
//        try {
//            // Initialize the MsConfig Component.
//            arguments = args;
//            MsConfig.init(args);
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        }
//
//        Kompics.createAndStart(SystemMain.class, numWorkers);
//    }
//}
