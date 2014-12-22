package se.sics.ms.simulator;

import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.config.*;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.network.model.king.KingLatencyMap;
import se.sics.gvod.p2p.simulator.P2pSimulator;
import se.sics.gvod.p2p.simulator.P2pSimulatorInit;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;

import java.io.IOException;

public final class SearchSimulationMain extends ComponentDefinition {

    private static SimulationScenario scenario = SimulationScenario.load(System
            .getProperty("scenario"));
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
                }
            }
        });
    }

    public SearchSimulationMain() throws IOException {
        P2pSimulator.setSimulationPortType(SimulatorPort.class);

        VodConfig.init(new String[0]);

//        CroupierConfiguration croupierConfig =
//                (CroupierConfiguration) AbstractConfiguration.load(CroupierConfiguration.class);
        CroupierConfiguration croupierConfig = CroupierConfiguration.build()
                .setRto(3000)
                .setRtoRetries(2)
                .setRtoScale(1.0d);

        // TODO - check that this is the correct seed being passed to the KingLatencyMap
        Component p2pSimulator = create(P2pSimulator.class, new P2pSimulatorInit(simulatorScheduler,
                scenario, new KingLatencyMap(croupierConfig.getSeed())));
        Component simulator = create(SearchSimulator.class, new SearchSimulatorInit(
                croupierConfig,
                GradientConfiguration.build(),
                SearchConfiguration.build(),
                ElectionConfiguration.build(),
                ChunkManagerConfiguration.build()));

        // connect
        connect(simulator.getNegative(VodNetwork.class), p2pSimulator.getPositive(VodNetwork.class));
        connect(simulator.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
        connect(simulator.getNegative(SimulatorPort.class), p2pSimulator.getPositive(SimulatorPort.class));
    
    }
}
