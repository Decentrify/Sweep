package se.sics.ms.simulator;

import java.io.IOException;
import java.net.InetAddress;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.network.model.king.KingLatencyMap;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.jetty.JettyWebServer;

import se.sics.gvod.config.VodConfig;
import se.sics.gvod.p2p.simulator.P2pSimulator;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.ms.configuration.ElectionConfiguration;
import se.sics.ms.configuration.SearchConfiguration;
import se.sics.ms.simulation.SimulatorPort;

public final class SearchExecutionMain extends ComponentDefinition {

	private static SimulationScenario scenario = SimulationScenario.load(System
			.getProperty("scenario"));

	public static void main(String[] args) {

		Kompics.createAndStart(SearchExecutionMain.class, 1);
	}

	public SearchExecutionMain() throws IOException {
//        DistributedOrchestratorNat.setSimulationPortType(SimulatorPort.class);
            P2pSimulator.setSimulationPortType(SimulatorPort.class);

                VodConfig.init(new String[0]);
                
		// create
		Component p2pSimulator = create(P2pSimulator.class);
//		Component p2pOrchestrator = create(DistributedOrchestratorNat.class);
		Component simulator = create(SearchSimulator.class);
		Component web = create(JettyWebServer.class);

		final BootstrapConfiguration bootConfiguration = BootstrapConfiguration.load(System
				.getProperty("bootstrap.configuration"));
        final CroupierConfiguration croupierConfiguration = CroupierConfiguration.build();
		final SearchConfiguration searchConfiguration = SearchConfiguration.load(System
				.getProperty("search.configuration"));
//		final GradientConfiguration gradientConfiguration = GradientConfiguration.build();
		final ElectionConfiguration electionConfiguration = ElectionConfiguration.load(System
				.getProperty("election.configuration"));

		trigger(new SimulatorInit(bootConfiguration, croupierConfiguration, gradientConfiguration,
				searchConfiguration, electionConfiguration), simulator.getControl());

		// connect
		connect(simulator.getNegative(VodNetwork.class), p2pSimulator.getPositive(VodNetwork.class));
		connect(simulator.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
		connect(simulator.getNegative(SimulatorPort.class),
				p2pSimulator.getPositive(SimulatorPort.class));
		connect(simulator.getPositive(Web.class), web.getNegative(Web.class));

		InetAddress ip = InetAddress.getLocalHost();
		int webPort = 9999;
//		String webServerAddr = "http://" + ip.getHostAddress() + ":" + webPort;
//		final JettyWebServerConfiguration webConfiguration = new JettyWebServerConfiguration(ip,
//				webPort, 30 * 1000, 2, webServerAddr);
//		trigger(new JettyWebServerInit(webConfiguration), web.getControl());
//		System.out.println("Webserver Started. Address=" + webServerAddr + "/1/search");

		// Must init DistributedOrchestrator last of all components, otherwise events
		// will be dropped
		trigger(new P2pSimulatorInit(new SimulatorScheduler(), 
                        scenario, new KingLatencyMap(croupierConfiguration.getSeed())),
				p2pSimulator.getControl());
//		trigger(new DistributedOrchestratorInit(scenario, new KingLatencyMap(croupierConfiguration.getSeed()), ip, webPort),
//				p2pSimulator.getControl());
	}
}
