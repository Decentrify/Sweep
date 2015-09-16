package se.sics.ms.launch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.serializer.SerializerHelper;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.helper.SystemConfigBuilder;

import java.io.IOException;

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

        SystemConfigBuilder builder = new SystemConfigBuilder(config);
        SystemConfig systemConfig = builder.build();
        GradientConfig gradientConfig  = new GradientConfig(config);
        CroupierConfig croupierConfig = new CroupierConfig(config);
        ElectionConfig electionConfig = new ElectionConfig(config);
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig(config);
        TreeGradientConfig treeGradientConfig = new TreeGradientConfig(config);
        
        logger.debug(" Loaded the configurations ... ");

        timer = create(JavaTimer.class, Init.NONE);
        network = create(NettyNetwork.class, new NettyInit(systemConfig.self));
        searchPeer = create(SearchPeer.class, new SearchPeerInit(systemConfig, croupierConfig,
                SearchConfiguration.build(), GradientConfiguration.build(),
                chunkManagerConfig, gradientConfig, electionConfig, treeGradientConfig ));

        connect(timer.getPositive(Timer.class), searchPeer.getNegative(Timer.class));
        connect(network.getPositive(Network.class), searchPeer.getNegative(Network.class));

        logger.debug("All components booted up ...");
        
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
    }


    /**
     * Perform the main initialization tasks.
     */
    private void doInit() {

        int startId = 128;

        logger.debug("Init of main launch invoked ...");
        logger.debug("Loading the main configuration file");
        config = ConfigFactory.load("application.conf");

        logger.debug("Setting up the serializers");
        SerializerHelper.registerSerializers(startId);
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





    public static void main(String[] args) throws IOException {

        int cores = Runtime.getRuntime().availableProcessors();
        int numWorkers = Math.max(1, cores - 1);

//        MsConfig.init(args);
        System.setProperty("java.net.preferIPv4Stack", "true");
        Kompics.createAndStart(SystemLaunch.class, numWorkers);
    }



}
