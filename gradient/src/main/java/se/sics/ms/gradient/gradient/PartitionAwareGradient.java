package se.sics.ms.gradient.gradient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierSample;
import se.sics.p2ptoolbox.gradient.GradientComp;
import se.sics.p2ptoolbox.gradient.msg.GradientShuffle;
import se.sics.p2ptoolbox.gradient.util.GradientLocalView;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

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
    private ApplicationSelf self;
    private SystemConfig systemConfig;
    
    // PORTS.
    private Positive<Timer> timerPositive = requires(Timer.class);
    private Positive<Network> networkPositive = requires(Network.class);
    private Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);

    public PartitionAwareGradient(PAGInit init){
        
        doInit(init);
        subscribe(startHandler, control);
        subscribe(croupierSampleHandler, croupierPortPositive);
        subscribe(handleShuffleRequest, networkPositive);
        subscribe(handleShuffleResponse, networkPositive);
    }

    
    /**
     * Initializer for the Partition Aware Gradient.
     * @param init init
     */
    private void doInit(PAGInit init) {
        
        logger.debug("Initializing the Partition Aware Gradient");
        
        systemConfig = init.getSystemConfig();
        
        // Gradient Connections.
        GradientComp.GradientInit gInit = new GradientComp.GradientInit(
                systemConfig, 
                init.getGradientConfig(),
                0,
                new SimpleUtilityComparator(), 
                new SweepGradientFilter());
        
        gradient = create(GradientComp.class, gInit);
        connect(gradient.getNegative(Timer.class), timerPositive);
    }

    
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("{}: Partition Aware Gradient Initialized ... ");
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
            logger.debug("{}: Received Croupier Sample");
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
    ClassMatchedHandler handleShuffleRequest
            = new ClassMatchedHandler<GradientShuffle.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request>>() {

        @Override
        public void handle(GradientShuffle.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Request> context) {
        }
    };

    /**
     * Same implementation as above but for the Shuffle Response.
     */
    ClassMatchedHandler handleShuffleResponse
            = new ClassMatchedHandler<GradientShuffle.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response>>() {

        @Override
        public void handle(GradientShuffle.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, GradientShuffle.Response> context) {
            logger.debug("{}: Received gradient shuffle response from the node :{}", context.getSource());
        }
    };
}

