package se.sics.ms.main;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.cc.sim.CCHeartbeatSimComp;
import se.sics.ktoolbox.cc.sim.CCHeartbeatSimInit;
import se.sics.ktoolbox.cc.sim.CCSimMain;
import se.sics.ktoolbox.cc.sim.CCSimMainInit;
import se.sics.ms.net.SerializerSetup;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Main launch class for the simulator.
 *
 * Created by babbar on 2015-08-18.
 */
public class SimulatorHostComp extends ComponentDefinition{

    private Logger logger = LoggerFactory.getLogger(SimulatorHostComp.class);
    private Config config;
    private Component searchPeer;
    private Component caracalSimComp;
    private Component caracalSimHeartbeatComp;

    private static final int SLOT_LENGTH = 20;
    private DecoratedAddress ccAddress;

    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);


    public SimulatorHostComp(SimulatorHostCompIInit init) {

        doInit(init);
        subscribe(startHandler, control);
    }

    private void doInit(SimulatorHostCompIInit init) {

        logger.debug("Main simulation initialization.");
        ccAddress = init.ccAddress;

        logger.debug("Loading the main configuration file");
        config = ConfigFactory.load("application.conf");

        logger.debug("Setting up the serializers");
        int startId = 128;
        SerializerSetup.registerSerializers(startId);
    }


    /**
     * Main start handler for the system.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {

            logger.debug("Start handler invoked for the component.");

            SystemConfig systemConfig = new SystemConfig(config);
            GradientConfig gradientConfig  = new GradientConfig(config);
            CroupierConfig croupierConfig = new CroupierConfig(config);
            ElectionConfig electionConfig = new ElectionConfig(config);
            ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig(config);
            TreeGradientConfig treeGradientConfig = new TreeGradientConfig(config);

            logger.debug(" Loaded the configurations ... ");

//          Need to perform the creation of the caracal client in the simulator version and then make the connections.
            bootCaracalSimClient(systemConfig.self, ccAddress);

            searchPeer = create(SearchPeer.class, new SearchPeerInit(systemConfig, croupierConfig,
                    SearchConfiguration.build(), GradientConfiguration.build(),
                    chunkManagerConfig, gradientConfig, electionConfig, treeGradientConfig ));

            connect(timer, searchPeer.getNegative(Timer.class));
            connect(network, searchPeer.getNegative(Network.class));
            connect(caracalSimHeartbeatComp.getPositive(CCHeartbeatPort.class), searchPeer.getNegative(CCHeartbeatPort.class));

            trigger(Start.event, searchPeer.getControl());

            logger.debug("All components booted up ...");
        }
    };


    /**
     * Boot the simulator version of the
     * caracal main and the heartbeat client.
     */
    private void bootCaracalSimClient ( DecoratedAddress selfAddress, DecoratedAddress caracalClientAddress ){

        caracalSimComp = create(CCSimMain.class, new CCSimMainInit(SLOT_LENGTH, caracalClientAddress));
        connect(caracalSimComp.getNegative(Timer.class), timer);
        connect(caracalSimComp.getNegative(Network.class), network);

        caracalSimHeartbeatComp = create(CCHeartbeatSimComp.class, new CCHeartbeatSimInit(selfAddress, caracalClientAddress));
        connect(caracalSimHeartbeatComp.getNegative(Timer.class), timer);
        connect(caracalSimHeartbeatComp.getNegative(Network.class), network);

        trigger(Start.event, caracalSimComp.getControl());
        trigger(Start.event, caracalSimHeartbeatComp.getControl());
    }



}
