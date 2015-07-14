package se.sics.ms.simulator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import se.sics.gvod.config.*;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;

import java.io.IOException;

import se.sics.kompics.network.Network;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.kompics.timer.Timer;
import se.sics.ms.configuration.MsConfig;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.simulator.core.P2pSimulator;
import se.sics.p2ptoolbox.simulator.core.P2pSimulatorInit;
import se.sics.p2ptoolbox.simulator.core.network.impl.UniformRandomModel;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;

public final class SearchSimulationMain extends ComponentDefinition {

    private static SimulationScenario scenario = SimulationScenario.load(System.getProperty("scenario"));
    private static SimulatorScheduler simulatorScheduler = new SimulatorScheduler();

    public static void main(String[] args) {

        Kompics.setScheduler(simulatorScheduler);
        Kompics.createAndStart(SearchSimulationMain.class, 1);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Kompics.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public SearchSimulationMain() throws IOException {

        P2pSimulator.setSimulationPortType(SimulatorPort.class);
        VodConfig.init(new String[0]);

        Component p2pSimulator = create(P2pSimulator.class, new P2pSimulatorInit(simulatorScheduler,
                scenario, new UniformRandomModel(1,10)));

        // TODO: SET THE SERIALIZATION MAIN.

        Config config = ConfigFactory.load("application.conf");

        CroupierConfig newCroupierConfig = new CroupierConfig(config);
        GradientConfig gradientConfig = new GradientConfig(config);
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig(config);
        ElectionConfig electionConfig = new ElectionConfig(config);
        TreeGradientConfig treeGradientConfig = new TreeGradientConfig(config);

        Component simulator = create(SearchSimulator.class, new SearchSimulatorInit(
                newCroupierConfig,
                GradientConfiguration.build(),
                SearchConfiguration.build(),
                ElectionConfiguration.build(),
                chunkManagerConfig,
                gradientConfig,
                electionConfig,
                treeGradientConfig));

        // connections
        connect(simulator.getNegative(Network.class), p2pSimulator.getPositive(Network.class));
        connect(simulator.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
        connect(simulator.getNegative(SimulatorPort.class), p2pSimulator.getPositive(SimulatorPort.class));

    }
}
