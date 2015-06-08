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
import se.sics.ms.gradient.events.AwaitVerificationTimeout;
import se.sics.ms.gradient.events.LUCheck;
import se.sics.ms.gradient.events.NPTimeout;
import se.sics.ms.gradient.events.PAGUpdate;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.PAGPort;
import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.SearchDescriptor;
import se.sics.ms.util.CommonHelper;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierSample;
import se.sics.p2ptoolbox.croupier.msg.CroupierUpdate;
import se.sics.p2ptoolbox.gradient.GradientComp;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientShuffle;
import se.sics.p2ptoolbox.gradient.util.GradientLocalView;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

import java.util.*;

/**
 * Main component for the exerting tight control over the gradient and the
 * croupier components in terms of analyzing samples and descriptors selected
 * to exchange data with.
 *
 * Created by babbarshaer on 2015-06-03.
 */
public class PartitionAwareGradient extends ComponentDefinition {


    private Logger logger = LoggerFactory.getLogger(PartitionAwareGradient.class);
    private Component gradient;
    private SystemConfig systemConfig;
    private SearchDescriptor selfDescriptor;
    private String prefix;
    private LeaderUnit lastLeaderUnit;
    private Set<Pair<Long, Integer>> verifiedSet;
    private Set<Pair<Long, Integer>> suspects;
    private Collection<DecoratedAddress> pnpNodes; // Possible Network Partitioned Nodes.
    private Map<UUID, DecoratedAddress> awaitingVerificationSelf;           // TO DO: Clear it on the leader unit switch, so as to invalidate all the responses.
    private Map<UUID, DecoratedAddress>awaitingVerificationSystem;
    
    private DecoratedAddress selfAddress;
    private int overlayId;
    
    // PORTS.
    private Positive<Timer> timerPositive = requires(Timer.class);
    private Positive<Network> networkPositive = requires(Network.class);
    private Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    private Negative<PAGPort> pagPortNegative = provides(PAGPort.class);
    private Negative<GradientPort> gradientPortNegative = provides(GradientPort.class);


    public PartitionAwareGradient(PAGInit init){

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
     * @param init init
     */
    private void doInit(PAGInit init) {
        
        logger.debug(" Initializing the Partition Aware Gradient ");

        prefix = String.valueOf(init.getBasicAddress().getId());
        systemConfig = init.getSystemConfig();
        selfAddress = systemConfig.self;
        overlayId = init.getOverlayId();
        
        awaitingVerificationSelf = new HashMap<UUID, DecoratedAddress>();
        awaitingVerificationSystem = new HashMap<UUID, DecoratedAddress>();
        
        // Gradient Connections.
        GradientComp.GradientInit gInit = new GradientComp.GradientInit(
                systemConfig, 
                init.getGradientConfig(),
                overlayId,
                new SimpleUtilityComparator(), 
                new SweepGradientFilter());
        
        verifiedSet = new HashSet<Pair<Long, Integer>>();
        suspects = new HashSet<Pair<Long, Integer>>();
        
        gradient = create(GradientComp.class, gInit);
        connect(gradient.getNegative(Timer.class), timerPositive);
        connect(gradient.getPositive(GradientPort.class), gradientPortNegative);    // Auxiliary Port for Direct Transfer of Data.
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
     * Network Partition timeout handler. Push to the application a list 
     * of the potential network partitioned nodes in the system.
     */
    Handler<NPTimeout> npTimeoutHandler = new Handler<NPTimeout>() {
        
        @Override
        public void handle(NPTimeout event) {
            
            logger.debug("{}: Timeout for handing over the potential network partitioned nodes to the application", prefix);
            throw new UnsupportedOperationException("Operation not supported yet.");
        }
    };
    

    /**
     * Simple handler for the self update
     * which is pushed by the application whenever the value
     * in self descriptor changes.
     * 
     */
    Handler<PAGUpdate> updateHandler = new Handler<PAGUpdate>() {
        @Override
        public void handle(PAGUpdate event) {

            logger.debug(" {}: Received update from the application ", prefix);
            selfDescriptor = event.getSelfView();
            LeaderUnit unit = selfDescriptor.getLastLeaderUnit();
            
            if(unit.getEpochId() != lastLeaderUnit.getEpochId() 
                    || unit.getLeaderId() != lastLeaderUnit.getLeaderId()) {

                // Clear the verified list if the last leader unit changes.
                verifiedSet.clear();
            }
            
            lastLeaderUnit = unit;
        }
    };

    /**
     * Blocks the direct update from the gradient component to the croupier
     * and relays it through this handler.
     */
    Handler<CroupierUpdate> croupierUpdateHandler = new Handler<CroupierUpdate>() {
        @Override
        public void handle(CroupierUpdate event) {
            
            logger.debug("{}: Received croupier update from the gradient. ", prefix);
            trigger(event, croupierPortPositive);
        }
    };
    
    
    /**
     * Handler that intercepts the sample from Croupier and then looks into the sample,
     * to filter them into safe and unsafe samples. The safe samples are allowed to pass through while 
     * the unsafe samples are blocked and handed over to the application after verification.
     *
     */
    Handler<CroupierSample<GradientLocalView>> croupierSampleHandler = new Handler<CroupierSample<GradientLocalView>>() {
        @Override
        public void handle(CroupierSample<GradientLocalView> event) {

            logger.debug("{}: Received sample from croupier ", prefix);

            // TO DO : Iterate through the sample to check for the unverified nodes.
            if(lastLeaderUnit != null){
                
                Set<Container<DecoratedAddress, GradientLocalView>> suspects =
                        new HashSet<Container<DecoratedAddress, GradientLocalView>>();
                                
                Set<Container<DecoratedAddress, GradientLocalView>> publicSample = event.publicSample;
                Set<Container<DecoratedAddress, GradientLocalView>> privateSample = event.privateSample;

                updateSuspects(publicSample, suspects);
                updateSuspects(privateSample, suspects);
                
                event = new CroupierSample<GradientLocalView>(event.overlayId, publicSample, privateSample);

                handleSuspects(suspects);
            }
            trigger(event, gradient.getNegative(CroupierPort.class));
        }
    };


    /**
     * Once the events are received, nodes create a unverified list in which the
     * descriptors are not verified. The method analyzes the descriptors 
     * and based on there last leader unit either sends them to application or through the
     * network to the other node.
     *
     * @param suspects suspects for NP.
     */
    private void handleSuspects(Set<Container<DecoratedAddress, GradientLocalView>> suspects){
        
        if(lastLeaderUnit == null){
            throw new IllegalStateException(" Method should not have been remove. ");
        }

        for(Container<DecoratedAddress, GradientLocalView> suspect : suspects){
            
            GradientLocalView glv = suspect.getContent();
            SearchDescriptor sd = (SearchDescriptor) glv.appView;
            
            LeaderUnit unit = sd.getLastLeaderUnit();

            ScheduleTimeout st = new ScheduleTimeout(3000);
            AwaitVerificationTimeout awt = new AwaitVerificationTimeout(st);
            st.setTimeoutEvent(awt);

            UUID requestId = st.getTimeoutEvent().getTimeoutId();
            LUCheck.Request request = new LUCheck.Request(requestId, unit.getEpochId(), unit.getLeaderId());
            
            if(unit.getEpochId() <  lastLeaderUnit.getEpochId()){
                
                // Send to application.
                trigger(request, pagPortNegative);
            }
            
            else {
                
                // Send through the network.
                BasicContentMsg requestMsg = CommonHelper.getDecoratedMsgWithOverlay(selfAddress, 
                        suspect.getSource(), Transport.UDP, 
                        overlayId, request);
                
                trigger(requestMsg, networkPositive);
            }
            
            
            awaitingVerificationSelf.put(requestId, suspect.getSource());
            trigger(st, timerPositive);
        }

    }


    /**
     * Handler for the Leader Unit Check response from the application.
     */
    Handler<LUCheck.Response> applicationLUCheckResponse = new Handler<LUCheck.Response>() {
        @Override
        public void handle(LUCheck.Response event) {
            
            logger.debug("{}: Received leader unit check response from the application", prefix);
            
            UUID requestId = event.getRequestId();
            if(awaitingVerificationSelf.containsKey(requestId)){
                
                // Response from application as part of request originated by self.
                // Add to the verified list.
                
                if(event.isVerified()){
                    
                    // What happens if unverified ??    
                    verifiedSet.add(Pair.with(event.getEpochId(), 
                            event.getLeaderId()));
                }
                else {
                    throw new UnsupportedOperationException("NP Nodes case needs to be handled.");
                }
                
                cancelTimeout(requestId);
                awaitingVerificationSelf.remove(requestId);
                
            }
            
            else if(awaitingVerificationSystem.containsKey(requestId)){
                
                // Response from application as part of request originated by node in system.
                DecoratedAddress dest = awaitingVerificationSystem.get(requestId);
                BasicContentMsg response = CommonHelper.getDecoratedMsgWithOverlay(selfAddress,
                        dest, Transport.UDP, 
                        overlayId, event);
                
                trigger(response, networkPositive);
                
                cancelTimeout(requestId);
                awaitingVerificationSystem.remove(requestId);
                
            }
            
            else {
                logger.debug("{}: Received leader unit check response for an already expired round", prefix);
            }
            
        }
    };
    
    
    
    private void cancelTimeout(UUID timeoutId){

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
    
    
    ClassMatchedHandler<LUCheck.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Request>> luCheckRequestHandler = 
            new ClassMatchedHandler<LUCheck.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Request>>() {
                
        @Override
        public void handle(LUCheck.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Request> context) {
            
            logger.debug("{}: Leader Unit Check Request Received from the Node in System", prefix);
            
            if(lastLeaderUnit == null || lastLeaderUnit.getEpochId() < content.getEpochId()){
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
    
    
    ClassMatchedHandler<LUCheck.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Response>> luCheckResponseHandler = 
            new ClassMatchedHandler<LUCheck.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Response>>() {
                
        @Override
        public void handle(LUCheck.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LUCheck.Response> context) {
            
            logger.debug("{}: Leader Unit Check Response Received from the node in the System.", prefix);
            UUID requestId = content.getRequestId();
            
            if(!awaitingVerificationSelf.containsKey(requestId)){
                logger.debug("{}: Round for which the check response received over network has already expired.", prefix);
                return;
            }
            
            cancelTimeout(requestId);
            awaitingVerificationSelf.remove(requestId);
            
            if(content.isVerified()){
                verifiedSet.add(Pair.with(content.getEpochId(), content.getLeaderId()));
            }
            else{
                throw new UnsupportedOperationException("Operation for np nodes is not handled yet");
            }
        }
    };
    
    

    /**
     * Update the suspected descriptors based on the verified set 
     * and the current last leader unit.
     *
     * @param baseSet set to check
     * @param suspects set to add
     */
    private void updateSuspects(Set<Container<DecoratedAddress, GradientLocalView>> baseSet, Set<Container<DecoratedAddress, GradientLocalView>> suspects){

        Iterator<Container<DecoratedAddress, GradientLocalView>> itr = baseSet.iterator();
        
        while(itr.hasNext()) {
            
            Container<DecoratedAddress, GradientLocalView> next = itr.next();
            LeaderUnit lastUnit = ((SearchDescriptor)next.getContent().appView)
                    .getLastLeaderUnit();

            if(lastUnit != null) {

                Pair<Long, Integer> pair = Pair.with(
                        lastUnit.getEpochId(), lastUnit.getLeaderId());

                if(!verifiedSet.contains(pair)){
                    suspects.add(next);
                    itr.remove();
                }
            }
            
            
        }
    }
    

    /**
     *
     * Interceptor for the gradient shuffle request.
     * The component analyzes the node from which the shuffle request is 
     * received and only if the node feels safe, then it is allowed to pass else the request is dropped.
     * <br/>
     * In some cases it might be really difficult to determine if based on the current 
     * state of self the  node is good or bad. Therefore, the component will buffer the request and initiate 
     * a verification mechanism. After verification gets over, appropriate steps are taken.
     * 
     */
    ClassMatchedHandler handleShuffleRequestFromGradient
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request>>() {

        @Override
        public void handle(GradientShuffle.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request> context) {
            
            logger.debug("{}: Received Shuffle Request, from gradient,  forwarding it ... ", prefix);
            BasicContentMsg request = new BasicContentMsg(context.getHeader(), content);
            trigger(request, networkPositive);
        }
    };

    /**
     * Same implementation as above but for the Shuffle Response.
     */
    ClassMatchedHandler handleShuffleResponseFromGradient
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response>>() {

        @Override
        public void handle(GradientShuffle.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response> context) {
            
            logger.debug("{}: Received gradient shuffle response, forwarding it ...", prefix);
            BasicContentMsg response = new BasicContentMsg(context.getHeader(), content);
            trigger(response, networkPositive);
        }
    };




    /**
     * Interceptor for the gradient shuffle request.
     * The component analyzes the node from which the shuffle request is
     * received and only if the node feels safe, then it is allowed to pass else the request is dropped.
     * <br/>
     * In some cases it might be really difficult to determine if based on the current
     * state of self the  node is good or bad. Therefore, the component will buffer the request and initiate
     * a verification mechanism. After verification gets over, appropriate steps are taken.
     *
     */
    ClassMatchedHandler handleShuffleRequestFromNetwork
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request>>() {

        @Override
        public void handle(GradientShuffle.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request> context) {

            logger.debug("{}: Received Shuffle Request, from network..  forwarding it ... ", prefix);
            BasicContentMsg request = new BasicContentMsg(context.getHeader(), content);
            trigger(request, gradient.getNegative(Network.class));
        }
    };

    /**
     * Same implementation as above but for the Shuffle Response.
     */
    ClassMatchedHandler handleShuffleResponseFromNetwork
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response>>() {

        @Override
        public void handle(GradientShuffle.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response> context) {

            logger.debug("{}: Received gradient shuffle response, forwarding it ...", prefix);
            BasicContentMsg response = new BasicContentMsg(context.getHeader(), content);
            trigger(response, gradient.getNegative(Network.class));
        }
    };


}

