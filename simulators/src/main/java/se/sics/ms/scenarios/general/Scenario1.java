package se.sics.ms.scenarios.general;

import se.sics.ms.simulation.Operations;

/**
 * Basic scenario simply initiating the system with 100 nodes.
 */
@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
	private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
		{
			StochasticProcess joinNodes = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(99, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
				}
			};

			joinNodes.start();
		}
	};

	public Scenario1() {
		super(scenario);
	}
}
