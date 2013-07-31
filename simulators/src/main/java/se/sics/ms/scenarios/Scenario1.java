package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 * Basic scenario simply initiating the system with 100 nodes.
 */
@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
	private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
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
			
			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
		}
	};

	public Scenario1() {
		super(scenario);
	}
}
