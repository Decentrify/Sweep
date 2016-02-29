package se.sics.ms.gradient.gradient;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.misc.CroupierContainerWrapper;
import se.sics.ms.gradient.misc.GradientShuffleWrapper;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.PAGPort;
import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.CommonHelper;
import se.sics.ms.util.PartitionHelper;
import java.util.*;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientComp;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.msg.GradientShuffle;
import se.sics.ktoolbox.gradient.util.GradientContainer;
import se.sics.ktoolbox.gradient.util.GradientLocalView;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;
import se.sics.ktoolbox.util.network.nat.NatType;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.update.view.ViewUpdate;
import se.sics.ktoolbox.util.update.view.ViewUpdatePort;

/**
 * Main component for the exerting tight control over the gradient and the
 * croupier components in terms of analyzing samples and descriptors selected to
 * exchange data with.
 *
 * Created by babbarshaer on 2015-06-03.
 */
public class PartitionAwareGradient extends ComponentDefinition {

    private Logger logger = LoggerFactory.getLogger(PartitionAwareGradient.class);
    private Component gradient;
    private SystemKCWrapper systemConfig;
    private PeerDescriptor selfDescriptor;
    private String prefix;
    private LeaderUnit lastLeaderUnit;
    private Queue<Pair<Long, Integer>> verifiedSet;
    private Set<Pair<Long, Integer>> suspects;
    private Map<Identifier, KAddress> pnpNodes; // Possible Network Partitioned Nodes.
    private Map<UUID, Pair<KAddress, Object>> awaitingVerificationSelf;           // TO DO: Clear it on the leader unit switch, so as to invalidate all the responses.
    private Map<UUID, KAddress> awaitingVerificationSystem;

    private KAddress selfAddress;
    private Identifier overlayId;

    // PORTS.
    private Positive<Timer> timerPositive = requires(Timer.class);
    private Positive<Network> networkPositive = requires(Network.class);
    private Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    private Positive<ViewUpdatePort> selfViewUPort = requires(ViewUpdatePort.class);
    private Negative<ViewUpdatePort> croupierViewUPort = provides(ViewUpdatePort.class);
    private Negative<PAGPort> pagPortNegative = provides(PAGPort.class);
    private Negative<GradientPort> gradientPortNegative = provides(GradientPort.class);

    public PartitionAwareGradient(PAGInit init) {

        doInit(init);

        subscribe(startHandler, control);
        subscribe(updateHandler, pagPortNegative);
        subscribe(npTimeoutHandler, timerPositive);

        subscribe(croupierUpdateHandler, gradient.getNegative(CroupierPort.class));
        subscribe(croupierSampleHandler, croupierPortPositive);

        subscribe(handleShuffleRequestFromNetwork, networkPositive);
        subscribe(handleShuffleResponseFromNetwork, networkPositive);

        subscribe(handleShuffleRequestFromGradient, gradient.getNegative(Network.class));
        subscribe(handleShuffleResponseFromGradient, gradient.getNegative(Network.class));

        subscribe(luCheckRequestHandler, networkPositive);
        subscribe(luCheckResponseHandler, networkPositive);
        subscribe(awaitVerificationTimeoutHandler, timerPositive);
        subscribe(applicationLUCheckResponse, pagPortNegative);

    }

    /**
     * Initializer for the Partition Aware Gradient.
     *
     * @param init init
     */
    private void doInit(PAGInit init) {

        logger.debug(" Initializing the Partition Aware Gradient ");

        prefix = init.selfAdr.getId().toString();
        systemConfig = new SystemKCWrapper(config());
        selfAddress = init.selfAdr;
        overlayId = init.overlayId;

        pnpNodes = new HashMap<Identifier, KAddress>();
        awaitingVerificationSelf = new HashMap<UUID, Pair<KAddress, Object>>();
        awaitingVerificationSystem = new HashMap<UUID, KAddress>();

        // Gradient Connections.
        GradientComp.GradientInit gInit = new GradientComp.GradientInit(overlayId, selfAddress, new SimpleUtilityComparator(),
                new SweepGradientFilter());

        verifiedSet = new Queue<Pair<Long, Integer>>(init.historyBufferSize);
        suspects = new HashSet<Pair<Long, Integer>>();

        gradient = create(GradientComp.class, gInit);
        connect(gradient.getNegative(Timer.class), timerPositive, Channel.TWO_WAY);
        connect(gradient.getPositive(GradientPort.class), gradientPortNegative, Channel.TWO_WAY);    // Auxiliary Port for Direct Transfer of Data.
    }

    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {

            logger.debug(" {}: Partition Aware Gradient Initialized ... ", prefix);
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(3000, 3000);
            NPTimeout npTimeout = new NPTimeout(spt);

            trigger(npTimeout, timerPositive);
        }
    };

    /**
     *
     * Network Partition timeout handler. Push to the application a list of the
     * potential network partitioned nodes in the system.
     */
    Handler<NPTimeout> npTimeoutHandler = new Handler<NPTimeout>() {

        @Override
        public void handle(NPTimeout event) {

            logger.debug("{}: Timeout for handing over the potential network partitioned nodes to the application", prefix);
            List<KAddress> npNodes = new ArrayList<KAddress>(pnpNodes.values());

            NPEvent npEvent = new NPEvent(npNodes);
            trigger(npEvent, pagPortNegative);

            pnpNodes.clear();
        }
    };

    /**
     * Simple handler for the self update which is pushed by the application
     * whenever the value in self descriptor changes.
     *
     */
    Handler<PAGUpdate> updateHandler = new Handler<PAGUpdate>() {
        @Override
        public void handle(PAGUpdate event) {

            logger.debug(" {}: Received update from the application ", prefix);
            selfDescriptor = event.getSelfView();
            LeaderUnit unit = selfDescriptor.getLastLeaderUnit();

            if (unit.getEpochId() != lastLeaderUnit.getEpochId()
                    || unit.getLeaderId() != lastLeaderUnit.getLeaderId()) {

                // Clear the verified list if the last leader unit changes.
                verifiedSet.clear();
            }

            lastLeaderUnit = unit;
        }
    };

    /**
     * Blocks the direct update from the gradient component to the croupier and
     * relays it through this handler.
     */
    Handler<ViewUpdate.Indication> croupierUpdateHandler = new Handler<ViewUpdate.Indication>() {
        @Override
        public void handle(ViewUpdate.Indication event) {

            logger.debug("{}: Received croupier update from the gradient. ", prefix);
            trigger(event, croupierViewUPort);
        }
    };

    /**
     * Handler that intercepts the sample from Croupier and then looks into the
     * sample, to filter them into safe and unsafe samples. The safe samples are
     * allowed to pass through while the unsafe samples are blocked and handed
     * over to the application after verification.
     *
     */
    Handler croupierSampleHandler = new Handler<CroupierSample<GradientLocalView>>() {
        @Override
        public void handle(CroupierSample<GradientLocalView> event) {

            logger.debug("{}: Received sample from croupier ", prefix);

            // TO DO : Iterate through the sample to check for the unverified nodes.
            if (lastLeaderUnit != null) {

                Map<Identifier, Container<KAddress, GradientLocalView>> suspects = new HashMap<>();
                Map<Identifier, Container<KAddress, GradientLocalView>> pubSample = new HashMap(event.publicSample);
                Map<Identifier, Container<KAddress, GradientLocalView>> privSample = new HashMap(event.privateSample);

                updateSuspectsMod(pubSample, suspects);
                updateSuspectsMod(privSample, suspects);

                event = new CroupierSample<GradientLocalView>(event.overlayId, pubSample, privSample);

                handleSuspects(event.overlayId, suspects);
            }
            trigger(event, gradient.getNegative(CroupierPort.class));
        }
    };

    /**
     * Once the events are received, nodes create a unverified list in which the
     * descriptors are not verified. The method analyzes the descriptors and
     * based on there last leader unit either sends them to application or
     * through the network to the other node.
     *
     * @param suspects suspects for NP.
     */
    private void handleSuspects(Identifier overlayId, Map<Identifier, Container<KAddress, GradientLocalView>> suspects) {

        if (lastLeaderUnit == null) {
            throw new IllegalStateException(" Method should not have been invoked. ");
        }

        for (Container<KAddress, GradientLocalView> suspect : suspects.values()) {

            GradientLocalView glv = suspect.getContent();
            PeerDescriptor sd = (PeerDescriptor) glv.appView;

            LeaderUnit unit = sd.getLastLeaderUnit();
            initiateLUCheckRequest(unit, suspect.getSource(), new CroupierContainerWrapper(suspect, overlayId));
        }

        // Well Do not include the partitioned node list to prevent sending the message
        // Because in case the reply from earlier set is not received on time then try again.
        // clear the suspects list. ( Only keep a verified list. )
        suspects.clear();
    }

    /**
     * Based on the supplied last leader unit and the suspected address the node
     * tries to determine the component to request for the verification of the
     * node.
     *
     * @param unit last unit
     * @param suspectAddress address
     */
    private void initiateLUCheckRequest(LeaderUnit unit, KAddress suspectAddress, Object content) {

        ScheduleTimeout st = new ScheduleTimeout(3000);
        AwaitVerificationTimeout awt = new AwaitVerificationTimeout(st);
        st.setTimeoutEvent(awt);

        UUID requestId = st.getTimeoutEvent().getTimeoutId();
        LUCheck.Request request = new LUCheck.Request(requestId, unit.getEpochId(), unit.getLeaderId());

        if (unit.getEpochId() < lastLeaderUnit.getEpochId()) {

            // Send to application.
            trigger(request, pagPortNegative);
        } else {

            // Send through the network.
            KContentMsg requestMsg = CommonHelper.getDecoratedMsgWithOverlay(selfAddress,
                    suspectAddress, Transport.UDP, overlayId, request);

            trigger(requestMsg, networkPositive);
        }

        awaitingVerificationSelf.put(requestId, Pair.with(suspectAddress, content));
        trigger(st, timerPositive);

    }

    /**
     * Handler for the Leader Unit Check response from the application.
     */
    Handler<LUCheck.Response> applicationLUCheckResponse = new Handler<LUCheck.Response>() {
        @Override
        public void handle(LUCheck.Response event) {

            logger.debug("{}: Received leader unit check response from the application", prefix);
            UUID requestId = event.getRequestId();

            if (awaitingVerificationSelf.containsKey(requestId)) {

                // Response from application as part of request originated by self.
                // Add to the verified list.
                if (event.isVerified()) {
                    verifiedSet.add(Pair.with(event.getEpochId(),
                            event.getLeaderId()));

                    Pair<KAddress, Object> associatedData = awaitingVerificationSelf.get(requestId);

                    KAddress address = associatedData.getValue0();
                    Object content = associatedData.getValue1();

                    if (content instanceof CroupierContainerWrapper) {

                        CroupierContainerWrapper ccw = ((CroupierContainerWrapper) content);
                        Container<KAddress, GradientLocalView> cc = ccw.container;
                        Map<Identifier, Container> privateS = new HashMap<>();
                        Map<Identifier, Container> publicS = new HashMap<>();

                        if (NatType.isOpen(address)) {
                            privateS.put(cc.getSource().getId(), cc);
                        } else {
                            publicS.put(cc.getSource().getId(), cc);
                        }

                        CroupierSample sample = new CroupierSample(ccw.overlayId, publicS, privateS);
                        trigger(sample, gradient.getNegative(CroupierPort.class));

                    } else if (content instanceof GradientShuffleWrapper) {

                        GradientShuffleWrapper gsw = (GradientShuffleWrapper) content;
                        KContentMsg requestMsg = new BasicContentMsg(gsw.header, gsw.content);
                        trigger(requestMsg, gradient.getNegative(Network.class));
                    }
                } else {

                    KAddress pnpNode = awaitingVerificationSelf.get(requestId).getValue0();
                    pnpNodes.put(pnpNode.getId(), pnpNode);
                }

                cancelTimeout(requestId);
                awaitingVerificationSelf.remove(requestId);
            } else if (awaitingVerificationSystem.containsKey(requestId)) {

                // Response from application as part of request originated by node in system.
                KAddress dest = awaitingVerificationSystem.get(requestId);
                BasicContentMsg response = CommonHelper.getDecoratedMsgWithOverlay(selfAddress,
                        dest, Transport.UDP,
                        overlayId, event);

                trigger(response, networkPositive);

                cancelTimeout(requestId);
                awaitingVerificationSystem.remove(requestId);

            } else {
                logger.debug("{}: Received leader unit check response for an already expired round", prefix);
            }

        }
    };

    /**
     * Convenient wrapper for the cancelling of timeout.
     *
     * @param timeoutId timeout id.
     */
    private void cancelTimeout(UUID timeoutId) {

        CancelTimeout ct = new CancelTimeout(timeoutId);
        trigger(ct, timerPositive);
    }

    /**
     * Timeout handler triggered for for the component waiting for the
     * verification of a suspected node from the application / other nodes.
     */
    Handler<AwaitVerificationTimeout> awaitVerificationTimeoutHandler = new Handler<AwaitVerificationTimeout>() {
        @Override
        public void handle(AwaitVerificationTimeout event) {

            logger.debug("{}: Await Verification Timeout triggered", prefix);

            // As the response is not received in time, simply remove the entry from the 
            // maps if any.
            UUID requestId = event.getTimeoutId();

            awaitingVerificationSelf.remove(requestId);
            awaitingVerificationSystem.remove(requestId);
        }
    };

    /**
     * The {@link se.sics.ms.gradient.events.LUCheck.Request} over the network
     * is a special case and triggered by a node when it thinks that based on
     * its current last leader unit, the unit that it wants to verify is ahead
     * of it and therefore sends a request. The metadata associated with the
     * request is stored separately and request is triggered to the application.
     */
    ClassMatchedHandler luCheckRequestHandler
            = new ClassMatchedHandler<LUCheck.Request, BasicContentMsg<KAddress, KHeader<KAddress>, LUCheck.Request>>() {

                @Override
                public void handle(LUCheck.Request content, BasicContentMsg<KAddress, KHeader<KAddress>, LUCheck.Request> context) {

                    logger.debug("{}: Leader Unit Check Request Received from the Node in System", prefix);

                    if (lastLeaderUnit == null || lastLeaderUnit.getEpochId() < content.getEpochId()) {
                        logger.debug("{}: Unable to determine as last leader unit currently behind the requested unit check.");
                        return;
                    }

                    ScheduleTimeout st = new ScheduleTimeout(3000);
                    AwaitVerificationTimeout awt = new AwaitVerificationTimeout(st);
                    st.setTimeoutEvent(awt);

                    UUID requestId = st.getTimeoutEvent().getTimeoutId();
                    LUCheck.Request request = new LUCheck.Request(requestId,
                            content.getEpochId(),
                            content.getLeaderId());

                    trigger(request, pagPortNegative);
                    trigger(st, timerPositive);

                    awaitingVerificationSystem.put(requestId, context.getSource());
                }
            };

    ClassMatchedHandler luCheckResponseHandler
            = new ClassMatchedHandler<LUCheck.Response, BasicContentMsg<KAddress, KHeader<KAddress>, LUCheck.Response>>() {

                @Override
                public void handle(LUCheck.Response content, BasicContentMsg<KAddress, KHeader<KAddress>, LUCheck.Response> context) {

                    logger.debug("{}: Leader Unit Check Response Received from the node in the System.", prefix);
                    UUID requestId = content.getRequestId();

                    if (!awaitingVerificationSelf.containsKey(requestId)) {
                        logger.debug("{}: Round for which the check response received over network has already expired.", prefix);
                        return;
                    }

                    cancelTimeout(requestId);
                    awaitingVerificationSelf.remove(requestId);

                    if (content.isVerified()) {
                        verifiedSet.add(Pair.with(content.getEpochId(), content.getLeaderId()));
                    } else {
                        throw new UnsupportedOperationException("Operation for np nodes is not handled yet");
                    }
                }
            };

    /**
     * Update the suspected descriptors based on the verified set and the
     * current last leader unit.
     *
     * @param baseSet set to check
     * @param suspects set to add
     */
    private void updateSuspects(Map<Identifier, Container<KAddress, GradientLocalView>> baseSet, Map<Identifier, Container<KAddress, GradientLocalView>> suspects) {

        Iterator<Container<KAddress, GradientLocalView>> itr = baseSet.values().iterator();

        while (itr.hasNext()) {

            Container<KAddress, GradientLocalView> next = itr.next();
            LeaderUnit lastUnit = ((PeerDescriptor) next.getContent().appView)
                    .getLastLeaderUnit();

            if (lastUnit != null) {

                Pair<Long, Integer> pair = Pair.with(
                        lastUnit.getEpochId(), lastUnit.getLeaderId());

                if (!verifiedSet.contains(pair)) {
                    suspects.put(next.getSource().getId(), next);
                    itr.remove();
                }
            }

        }
    }

    /**
     * Update the suspected descriptors based on the verified set and the
     * current last leader unit.
     *
     * @param baseSet set to check
     * @param suspects set to add
     */
    private void updateSuspectsMod(Map<Identifier, Container<KAddress, GradientLocalView>> baseSet, Map<Identifier, Container<KAddress, GradientLocalView>> suspects) {

        Iterator<Container<KAddress, GradientLocalView>> itr = baseSet.values().iterator();

        while (itr.hasNext()) {

            Container<KAddress, GradientLocalView> next = itr.next();

            PeerDescriptor descriptor = (PeerDescriptor) next.getContent().appView;
            LeaderUnit lastUnit = descriptor
                    .getLastLeaderUnit();

            if (lastUnit != null) {

                Pair<Long, Integer> pair = Pair.with(lastUnit.getEpochId(), lastUnit.getLeaderId());

                int selfOverlayId = selfDescriptor.getOverlayId().getId();
                int otherOverlayId = descriptor.getOverlayId().getId();

                if (PartitionHelper.isOverlayExtension(selfOverlayId, otherOverlayId, new IntIdentifier(descriptor.getId()))
                        && !verifiedSet.contains(pair)) {

                    // Only if the suspect is not present in the verified set and 
                    // an extension of my current overlay, then add it as a suspect.
                    // NOT ENABLING EXTN CHECK WOULD WRECK HAVOC. ( !!BEWARE!! )
                    suspects.put(next.getSource().getId(), next);
                    itr.remove();
                }
            }

        }
    }

    /**
     *
     * Interceptor for the gradient shuffle request. The component analyzes the
     * node from which the shuffle request is received and only if the node
     * feels safe, then it is allowed to pass else the request is dropped.
     * <br/>
     * In some cases it might be really difficult to determine if based on the
     * current state of self the node is good or bad. Therefore, the component
     * will buffer the request and initiate a verification mechanism. After
     * verification gets over, appropriate steps are taken.
     *
     */
    ClassMatchedHandler handleShuffleRequestFromGradient
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Request>>() {

                @Override
                public void handle(GradientShuffle.Request content, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Request> context) {

                    logger.debug("{}: Received Shuffle Request, from gradient,  forwarding it ... ", prefix);
                    BasicContentMsg request = new BasicContentMsg(context.getHeader(), content);
                    trigger(request, networkPositive);
                }
            };

    /**
     * Same implementation as above but for the Shuffle Response.
     */
    ClassMatchedHandler handleShuffleResponseFromGradient
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Response>>() {

                @Override
                public void handle(GradientShuffle.Response content, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Response> context) {

                    logger.debug("{}: Received gradient shuffle response, forwarding it ...", prefix);
                    BasicContentMsg response = new BasicContentMsg(context.getHeader(), content);
                    trigger(response, networkPositive);
                }
            };

    /**
     * Interceptor for the gradient shuffle request. The component analyzes the
     * node from which the shuffle request is received and only if the node
     * feels safe, then it is allowed to pass else the request is dropped.
     * <br/>
     * In some cases it might be really difficult to determine if based on the
     * current state of self the node is good or bad. Therefore, the component
     * will buffer the request and initiate a verification mechanism. After
     * verification gets over, appropriate steps are taken.
     *
     */
    ClassMatchedHandler handleShuffleRequestFromNetwork
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request>>() {

                @Override
                public void handle(GradientShuffle.Request content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request> context) {

                    logger.debug("{}: Received Shuffle Request, from network..  forwarding it ... ", prefix);

                    GradientContainer container = content.selfGC;

                    if (!(container.getContent() instanceof PeerDescriptor)) {
                        throw new IllegalStateException(" Gradient Shuffle state corrupted. ");
                    }

                    PeerDescriptor descriptor = (PeerDescriptor) container.getContent();
                    LeaderUnit lastUnit = descriptor.getLastLeaderUnit();

                    if (!verifiedSet.contains(Pair.with(lastUnit.getEpochId(), lastUnit.getLeaderId()))) {
                        initiateLUCheckRequest(lastUnit, context.getSource(), new GradientShuffleWrapper(content, context.getHeader()));
                    } else {
                        BasicContentMsg request = new BasicContentMsg(context.getHeader(), content);
                        trigger(request, gradient.getNegative(Network.class));
                    }
                }
            };

    /**
     * Same implementation as above but for the Shuffle Response.
     */
    ClassMatchedHandler handleShuffleResponseFromNetwork
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Response>>() {

                @Override
                public void handle(GradientShuffle.Response content, BasicContentMsg<KAddress, KHeader<KAddress>, GradientShuffle.Response> context) {

                    logger.debug("{}: Received gradient shuffle response, forwarding it ...", prefix);
                    BasicContentMsg response = new BasicContentMsg(context.getHeader(), content);
                    trigger(response, gradient.getNegative(Network.class));
                }
            };

    /**
     * A simple queue implementation in the system. The data is currently added
     * to the queue which has a max capacity. FIFO order is followed in order to
     * remove the element from the queue.
     *
     * @param <T>
     */
    private class Queue<T> {

        private LinkedList<T> queue;
        private int maxCapacity;

        public Queue(int maxCapacity) {

            if (!(maxCapacity >= 0)) {
                throw new IllegalStateException(" Max Capacity needs to be greater than 0 ");
            }

            this.maxCapacity = maxCapacity;
            this.queue = new LinkedList<T>();
        }

        /**
         * Simply add the element to the structure. Always keep a check on the
         * history size after we have added the element.
         *
         * @param data data to add.
         */
        public void add(T data) {

            if (data == null) {

                logger.debug("{}: Tried to add null element to queue");
                return;
            }

            int index = this.queue.indexOf(data);

            if (index != -1) {
                this.queue.set(index, data);
            } else {
                this.queue.add(data);
            }

            checkCapacityAndResize();
        }

        private void checkCapacityAndResize() {

            while (queue.size() >= maxCapacity) {
                queue.remove();
            }
        }

        public boolean contains(T data) {
            return this.queue.contains(data);
        }

        public void clear() {
            this.queue.clear();
        }

    }

}
