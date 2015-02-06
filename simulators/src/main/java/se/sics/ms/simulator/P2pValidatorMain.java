package se.sics.ms.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.config.*;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.ms.simulation.AddIndexEntry;
import se.sics.ms.simulation.IndexEntryP2pSimulated;
import se.sics.ms.simulation.PeerJoin;
import se.sics.ms.simulation.PeerJoinP2pSimulated;

import java.io.IOException;

/**
 * Created by babbarshaer on 2015-02-04.
 * Main validator class acting as a black-box. 
 */
public class P2pValidatorMain extends ComponentDefinition{

    private static final Logger logger = LoggerFactory.getLogger(P2pValidatorMain.class);
    
    Positive<VodNetwork> vodNetworkPositive = positive(VodNetwork.class);
    Positive<Timer> timerPositive = positive(Timer.class);
    VodAddress self;
    
    
    Component simulator = null;
    
    
    public P2pValidatorMain(P2pValidationMainInit init) throws IOException {
        logger.info("init");
//        VodConfig.init(new String[0]);
//        doInit(init);

        self = init.getSelf();
     
        logger.info("{}", self);
        // == Subscriptions.
        subscribe(startHandler, control);
        subscribe(indexEntryRequestHandler,vodNetworkPositive);
        subscribe(peerJoinRequestHandler, vodNetworkPositive);
    }

    /**
     * Initialize the component.
     * @param init
     */
    private void doInit(P2pValidationMainInit init) {

        // == Set self.
        self = init.getSelf();
        
        // == Build croupier configuration.
        CroupierConfiguration croupierConfig = CroupierConfiguration.build()
                .setRto(init.getRto())
                .setRtoRetries(init.getRtoTries())
                .setRtoScale(init.getRtoScale());

        // == BootUp the simulator.
        simulator = create(SearchSimulator.class, new SearchSimulatorInit(
                croupierConfig,
                GradientConfiguration.build(),
                SearchConfiguration.build(),
                ElectionConfiguration.build(),
                ChunkManagerConfiguration.build()));

        connect(simulator.getNegative(VodNetwork.class),vodNetworkPositive);
        connect(simulator.getNegative(Timer.class),timerPositive);

    }

    
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.info(" Starting the P2pValidatorMain component ... ");
            trigger(new PeerJoinP2pSimulated.Request(self, self, (long)self.getId()), vodNetworkPositive);
        }
    };
    
    // Can we invoke events on a port without forming a link to the port ?
    Handler<IndexEntryP2pSimulated.Request> indexEntryRequestHandler = new Handler<IndexEntryP2pSimulated.Request>() {
        @Override
        public void handle(IndexEntryP2pSimulated.Request event) {
            System.out.println(" Handle Index Entry Request Message.");
            trigger(new AddIndexEntry(event.getId()), simulator.getPositive(SimulatorPort.class));
        }
    };
    
    
    Handler<DirectMsg> peerJoinRequestHandler = new Handler<DirectMsg>(){

        @Override
        public void handle(DirectMsg event) {
            
            logger.info(" $$$$$$$$$ Received the Simulated Join Request. $$$$$$$$$$$");
//            trigger(new PeerJoin(event.getPeerId()), simulator.getPositive(SimulatorPort.class));
            
        }
    };
    
    

}
