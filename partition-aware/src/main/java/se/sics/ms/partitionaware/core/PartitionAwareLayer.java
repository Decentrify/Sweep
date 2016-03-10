package se.sics.ms.partitionaware.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.partitionaware.api.events.NPEvent;
import se.sics.ms.partitionaware.api.events.NPTimeout;
import se.sics.ms.partitionaware.api.events.PALUpdate;
import se.sics.ms.partitionaware.api.port.PALPort;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ms.types.PeerDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.msg.GradientShuffle;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;
import se.sics.ktoolbox.util.update.ViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * The Main Partition Aware Layer.
 * It acts as an interceptor between the
 * Created by babbarshaer on 2015-06-27.
 */
public class PartitionAwareLayer extends ComponentDefinition{
    private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareLayer.class);
    private String logPrefix;
    
//  Ports
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<Network> networkPositive = requires(Network.class);
    Negative<Network> networkNegative = provides(Network.class);
    Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    Negative<CroupierPort> croupierPortNegative = provides(CroupierPort.class);
    Positive<OverlayViewUpdatePort> selfViewUPort = requires(OverlayViewUpdatePort.class);
    Negative<OverlayViewUpdatePort> croupierViewUPort = provides(OverlayViewUpdatePort.class);
    Negative<PALPort> palPortNegative = provides(PALPort.class);
    
//  Local Variables.
    private KAddress selfBase;
    private PeerDescriptor selfDescriptor;
    private Set<KAddress> pnpNodes;
    
    public PartitionAwareLayer(PALInit init){

        doInit(init);
        subscribe(startHandler, control);
        subscribe(palUpdateHandler, palPortNegative);
        
        subscribe(npTimeoutHandler, timerPositive);
        
        subscribe(handleOutgoingShuffleRequest, networkNegative);
        subscribe(handleIncomingShuffleRequest, networkPositive);
        subscribe(handleOutgoingShuffleResponse, networkNegative);
        subscribe(handleIncomingShuffleResponse, networkPositive);
        
        subscribe(croupierUpdateHandler, selfViewUPort);
        subscribe(croupierSampleHandler, croupierPortPositive);
    }

    
    /**
     * Initialization Method.
     * @param init init
     */
    private void doInit(PALInit init) {
        
        selfBase = init.self;
        logPrefix = selfBase.getId().toString();
        pnpNodes = new HashSet<KAddress>();
    }


    /**
     * Handler indicating that the component
     * has been initialized and ready.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            
            LOG.info("{}: Partition Aware Layer booted up.", logPrefix);

            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(3000, 3000);
            NPTimeout npTimeout = new NPTimeout(spt);
            spt.setTimeoutEvent(npTimeout);
//            trigger(spt, timerPositive);
            
        }
    };
    
    
//  Application Interaction
//  --------------------------------------------------------------------------------------------------------------------
    
    Handler<PALUpdate> palUpdateHandler = new Handler<PALUpdate>() {
        @Override
        public void handle(PALUpdate event) {
            
            LOG.info("{}: Received Update from Application", logPrefix);
            selfDescriptor = event.getSelfView();
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

            LOG.debug("{}: Timeout for handing over the potential network partitioned nodes to the application", logPrefix);
            
            Collection<KAddress> npNodes = new ArrayList(pnpNodes);

            NPEvent npEvent = new NPEvent(npNodes);
            trigger(npEvent, palPortNegative);

            pnpNodes.clear();
        }
    };
    
    
    

//  Gradient Interaction Interception.
//  --------------------------------------------------------------------------------------------------------------------
    
    ClassMatchedHandler handleOutgoingShuffleRequest
            = new ClassMatchedHandler<GradientShuffle.Request, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request>>() {

        @Override
        public void handle(GradientShuffle.Request content, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request> context) {
            
            LOG.info("{}: Handle outgoing gradient shuffle request", logPrefix);
            trigger(context, networkPositive);
        }
    };
    
    
    ClassMatchedHandler handleIncomingShuffleRequest
            = new ClassMatchedHandler<GradientShuffle.Request, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request>>() {
        @Override
        public void handle(GradientShuffle.Request content, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Request> context) {
            LOG.info("{}: Handle incoming gradient shuffle request", logPrefix);
            trigger(context, networkNegative);
        }
    };


    ClassMatchedHandler handleOutgoingShuffleResponse
            = new ClassMatchedHandler<GradientShuffle.Response, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Response>>() {
        @Override
        public void handle(GradientShuffle.Response content, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Response> context) {
            LOG.info("{}:Handle outgoing shuffle response", logPrefix);
            trigger(context, networkPositive);
        }
    };
    
    

    ClassMatchedHandler handleIncomingShuffleResponse
            = new ClassMatchedHandler<GradientShuffle.Response, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Response>>() {
        @Override
        public void handle(GradientShuffle.Response content, KContentMsg<KAddress, DecoratedHeader<KAddress>, GradientShuffle.Response> context) {
            LOG.info("{}: Handle incoming shuffle response", logPrefix);
            trigger(context, networkNegative);
        }
    };
                
    

    
//  Croupier Interaction Interception.
//  --------------------------------------------------------------------------------------------------------------------
    /**
     * Handler of the update regarding the self view from the application to the 
     * croupier component directly.
     */
    Handler croupierUpdateHandler = new Handler<ViewUpdate.Indication>() {
        @Override
        public void handle(ViewUpdate.Indication event) {
            
            LOG.info("{}: Intercepting croupier update from gradient to croupier.", logPrefix);
            trigger(event, croupierViewUPort);
        }
    };


    /**
     * Handler that intercepts the sample from Croupier and then looks into the sample,
     * to filter them into safe and unsafe samples. The safe samples are allowed to pass through while 
     * the unsafe samples are blocked and handed over to the application after verification.
     *
     */
    Handler<CroupierSample> croupierSampleHandler = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            
            LOG.info("{}: Intercepting the croupier sample from the croupier to the gradient", logPrefix);
            trigger(event, croupierPortNegative);
        }
    };
    
}
