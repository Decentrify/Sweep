package search.simulator.core;

import java.io.IOException;
import java.net.InetAddress;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.network.model.common.NetworkModel;
import se.sics.gvod.p2p.orchestrator.distributed.DistributedOrchestratorInit;
import se.sics.gvod.p2p.orchestrator.distributed.DistributedOrchestratorNat;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.model.king.KingLatencyMap;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.jetty.JettyWebServer;
import se.sics.kompics.web.jetty.JettyWebServerConfiguration;
import se.sics.kompics.web.jetty.JettyWebServerInit;

import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;
import common.simulation.SimulatorInit;
import common.simulation.SimulatorPort;
import se.sics.gvod.config.RendezvousServerConfiguration;
import se.sics.gvod.config.VodConfig;

public final class SearchExecutionMain extends ComponentDefinition {

	private static SimulationScenario scenario = SimulationScenario.load(System
			.getProperty("scenario"));

	public static void main(String[] args) {

		Kompics.createAndStart(SearchExecutionMain.class, 1);
	}

	public SearchExecutionMain() throws IOException {
        DistributedOrchestratorNat.setSimulationPortType(SimulatorPort.class);

                VodConfig.init(new String[0]);
                
		// create
		Component p2pOrchestrator = create(DistributedOrchestratorNat.class);
		Component simulator = create(SearchSimulator.class);
		Component web = create(JettyWebServer.class);

		final BootstrapConfiguration bootConfiguration = BootstrapConfiguration.load(System
				.getProperty("bootstrap.configuration"));
        final CroupierConfiguration croupierConfiguration = CroupierConfiguration.build();
		final SearchConfiguration searchConfiguration = SearchConfiguration.load(System
				.getProperty("search.configuration"));
		final TManConfiguration tmanConfiguration = TManConfiguration.load(System
				.getProperty("tman.configuration"));
		final ElectionConfiguration electionConfiguration = ElectionConfiguration.load(System
				.getProperty("election.configuration"));

		trigger(new SimulatorInit(bootConfiguration, croupierConfiguration, tmanConfiguration,
				searchConfiguration, electionConfiguration), simulator.getControl());

		// connect
		connect(simulator.getNegative(VodNetwork.class), p2pOrchestrator.getPositive(VodNetwork.class));
		connect(simulator.getNegative(Timer.class), p2pOrchestrator.getPositive(Timer.class));
		connect(simulator.getNegative(SimulatorPort.class),
				p2pOrchestrator.getPositive(SimulatorPort.class));
		connect(simulator.getPositive(Web.class), web.getNegative(Web.class));

		InetAddress ip = InetAddress.getLocalHost();
		int webPort = 9999;
		String webServerAddr = "http://" + ip.getHostAddress() + ":" + webPort;
		final JettyWebServerConfiguration webConfiguration = new JettyWebServerConfiguration(ip,
				webPort, 30 * 1000, 2, webServerAddr);
		trigger(new JettyWebServerInit(webConfiguration), web.getControl());
		System.out.println("Webserver Started. Address=" + webServerAddr + "/1/search");

		// Must init P2pOrchestrator last of all components, otherwise events
		// will be dropped
		trigger(new DistributedOrchestratorInit(scenario, new NetworkModel() {
            @Override
            public long getLatencyMs(RewriteableMsg rewriteableMsg) {
                return 0;
            }
        }, ip, webPort),
				p2pOrchestrator.getControl());
	}
}
