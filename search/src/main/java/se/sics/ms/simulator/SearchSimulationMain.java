package se.sics.ms.simulator;

import java.io.IOException;
import se.sics.gvod.config.AbstractConfiguration;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.network.model.king.KingLatencyMap;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

import se.sics.gvod.config.VodConfig;
import se.sics.gvod.p2p.simulator.P2pSimulator;
import se.sics.gvod.p2p.simulator.P2pSimulatorInit;
import se.sics.kompics.simulation.SimulatorScheduler;

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

        Component p2pSimulator = create(P2pSimulator.class);
        Component simulator = create(SearchSimulator.class);
//		Component web = create(JettyWebServer.class);

        CroupierConfiguration croupierConfig =
                (CroupierConfiguration) AbstractConfiguration.load(CroupierConfiguration.class);

        // connect
        connect(simulator.getNegative(VodNetwork.class), p2pSimulator.getPositive(VodNetwork.class));
        connect(simulator.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
        connect(simulator.getNegative(SimulatorPort.class),
                p2pSimulator.getPositive(SimulatorPort.class));
//		connect(simulator.getPositive(Web.class), web.getNegative(Web.class));

//		InetAddress ip = InetAddress.getLocalHost();
//		int webPort = 9999;
//		String webServerAddr = "http://" + ip.getHostAddress() + ":" + webPort;
//		final JettyWebServerConfiguration webConfiguration = new JettyWebServerConfiguration(ip,
//				webPort, 30 * 1000, 2, webServerAddr);
//		trigger(new JettyWebServerInit(webConfiguration), web.getControl());
//		System.out.println("Webserver Started. Address=" + webServerAddr + "/1/search");

        // Must init DistributedOrchestrator last of all components, otherwise events
        // will be dropped

//		trigger(new DistributedOrchestratorInit(scenario, new KingLatencyMap(croupierConfiguration.getSeed()), ip, webPort),
//				p2pSimulator.getControl());

        trigger(new SimulatorInit(
                croupierConfig,
                (GradientConfiguration) AbstractConfiguration.load(GradientConfiguration.class),
                (SearchConfiguration) AbstractConfiguration.load(SearchConfiguration.class),
                (ElectionConfiguration) AbstractConfiguration.load(ElectionConfiguration.class)), simulator.getControl());

        trigger(new P2pSimulatorInit(simulatorScheduler,
                scenario, new KingLatencyMap(croupierConfig.getSeed())),
                p2pSimulator.getControl());
    
    }
}
