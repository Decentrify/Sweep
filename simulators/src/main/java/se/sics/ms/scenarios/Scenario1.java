package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 * Basic scenario simply initiating the system with 100 nodes.
 */
@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
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
					raise(1, Operations.peerJoin());
				}
			};
			
			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
		}
	};

	public Scenario1() {
		super(scenario);
	}
}
