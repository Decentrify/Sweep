package se.kth.ms.partitionaware.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.ms.partitionaware.api.events.PALUpdate;
import se.kth.ms.partitionaware.api.port.PALPort;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierSample;
import se.sics.p2ptoolbox.croupier.msg.CroupierUpdate;
import se.sics.p2ptoolbox.gradient.msg.GradientShuffle;
import se.sics.p2ptoolbox.gradient.util.GradientLocalView;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

/**
 * The Main Partition Aware Layer.
 * It acts as an interceptor between the
 * Created by babbarshaer on 2015-06-27.
 */
public class PartitionAwareLayer extends ComponentDefinition{
    
//  Ports
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<Network> networkPositive = requires(Network.class);
    Negative<Network> networkNegative = provides(Network.class);
    Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    Negative<CroupierPort> croupierPortNegative = provides(CroupierPort.class);
    Negative<PALPort> palPortNegative = provides(PALPort.class);
    
//  Local Variables.
    private Logger logger = LoggerFactory.getLogger(PartitionAwareLayer.class);
    private BasicAddress selfBase;
    private SearchDescriptor selfDescriptor;
    
    public PartitionAwareLayer(PALInit init){

        doInit(init);
        subscribe(startHandler, control);
        subscribe(palUpdateHandler, palPortNegative);
        
        subscribe(handleOutgoingShuffleRequest, networkNegative);
        subscribe(handleIncomingShuffleRequest, networkPositive);
        subscribe(handleOutgoingShuffleResponse, networkNegative);
        subscribe(handleIncomingShuffleResponse, networkPositive);
        
        subscribe(croupierUpdateHandler, croupierPortNegative);
        subscribe(croupierSampleHandler, croupierPortPositive);
    }

    
    /**
     * Initialization Method.
     * @param init init
     */
    private void doInit(PALInit init) {
        selfBase = init.selfBase;
    }


    /**
     * Handler indicating that the component
     * has been initialized and ready.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            
            logger.info("Partition Aware Layer booted up.");
        }
    };
    
    
//  Application Interaction
//  --------------------------------------------------------------------------------------------------------------------
    Handler<PALUpdate> palUpdateHandler = new Handler<PALUpdate>() {
        @Override
        public void handle(PALUpdate event) {
            
            logger.info("Received Update from Application");
            selfDescriptor = event.getSelfView();
        }
    };
    
    

//  Gradient Interaction Interception.
//  --------------------------------------------------------------------------------------------------------------------
    
    ClassMatchedHandler handleOutgoingShuffleRequest
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request>>() {

        @Override
        public void handle(GradientShuffle.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request> context) {
            
            logger.info("Handle outgoing gradient shuffle request");
            System.exit(-1);
            trigger(context, networkPositive);
        }
    };
    
    
    ClassMatchedHandler handleIncomingShuffleRequest
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request>>() {
        @Override
        public void handle(GradientShuffle.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request> context) {
            logger.info("Handle incoming gradient shuffle request");
        }
    };


    ClassMatchedHandler handleOutgoingShuffleResponse
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response>>() {
        @Override
        public void handle(GradientShuffle.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response> context) {
            logger.info("Handle outgoing shuffle response");
        }
    };
    
    

    ClassMatchedHandler handleIncomingShuffleResponse
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response>>() {
        @Override
        public void handle(GradientShuffle.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response> context) {
            logger.info("Handle incoming shuffle response");
        }
    };
                
    

    
//  Croupier Interaction Interception.
//  --------------------------------------------------------------------------------------------------------------------
    /**
     * Handler of the update regarding the self view from the application to the 
     * croupier component directly.
     */
    Handler<CroupierUpdate> croupierUpdateHandler = new Handler<CroupierUpdate>() {
        @Override
        public void handle(CroupierUpdate event) {
            
            logger.info("Intercepting croupier update from gradient to croupier.");
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
            
            logger.info("Intercepting the croupier sample from the croupier to the gradient");
        }
    };
    
}
