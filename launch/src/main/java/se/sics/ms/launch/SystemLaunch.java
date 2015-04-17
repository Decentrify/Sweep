package se.sics.ms.launch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.List;

/**
 *
 * Main Class for booting up the application.
 *
 * Created by babbar on 2015-04-17.
 */
public class SystemLaunch extends ComponentDefinition{

    Component network;
    Component timer;
    Component searchPeer;
    Config config;

    private Logger logger = LoggerFactory.getLogger(SystemLaunch.class);

    public SystemLaunch(){

        doInit();

        SystemConfig systemConfig = new SystemConfig(config);
        GradientConfig gradientConfig  = new GradientConfig(config);
        CroupierConfig croupierConfig = new CroupierConfig(config);
        ElectionConfig electionConfig = new ElectionConfig.ElectionConfigBuilder(MsConfig.GRADIENT_VIEW_SIZE).buildElectionConfig();

        ApplicationSelf applicationSelf = new ApplicationSelf(systemConfig.self);
        List<DecoratedAddress> bootstrapNodes = systemConfig.bootstrapNodes;

        timer = create(JavaTimer.class, Init.NONE);
        network = create(NettyNetwork.class, new NettyInit(systemConfig.self));
        searchPeer = create(SearchPeer.class, new SearchPeerInit(applicationSelf, systemConfig, croupierConfig,
                SearchConfiguration.build(), GradientConfiguration.build(),
                ElectionConfiguration.build(), ChunkManagerConfiguration.build(), gradientConfig, electionConfig ));

        connect(timer.getPositive(Timer.class), searchPeer.getNegative(Timer.class));
        connect(network.getPositive(Network.class), searchPeer.getNegative(Network.class));

        subscribe(startHandler, control);
        subscribe(stopHandler, control);
    }


    /**
     * Perform the main initialization tasks.
     */
    private void doInit() {

        logger.debug("Init of main launch invoked ...");
        logger.debug("Loading the main configuration file");

        config = ConfigFactory.load("application.conf");
        // MAIN SERIALIZER SETUP NEEDS TO GO HERE.
    }


    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {
            logger.trace("Component Started");
        }
    };


    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop stop) {
            logger.trace("Stopping Component.");
        }
    };

}
