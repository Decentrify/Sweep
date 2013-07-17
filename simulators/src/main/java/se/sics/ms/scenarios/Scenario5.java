package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 * Scenario adding real magnet links.
 */
@SuppressWarnings("serial")
public class Scenario5 extends Scenario {
	private static SimulationScenario scenario = new SimulationScenario() {
		{
			StochasticProcess startUp = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(1, Operations.peerJoin());
				}
			};

			StochasticProcess joinNodes = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(99, Operations.peerJoin());
				}
			};

			StochasticProcess addMagnetEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
					raise(100, Operations.addMagnetEntry(), uniform(0, 100));
				}
			};

			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
			addMagnetEntries.startAfterTerminationOf(5000, joinNodes);
		}
	};

	public Scenario5() {
		super(scenario);
	}
}
