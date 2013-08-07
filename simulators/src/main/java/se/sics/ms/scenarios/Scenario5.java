package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 * Scenario adding real magnet links.
 */
@SuppressWarnings("serial")
public class Scenario5 extends Scenario {
	private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
		{
			StochasticProcess joinNodes = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(100, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
				}
			};

			StochasticProcess addMagnetEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
					raise(100, Operations.addMagnetEntry(), uniform(0, Integer.MAX_VALUE));
				}
			};

			joinNodes.start();
			addMagnetEntries.startAfterTerminationOf(5000, joinNodes);
		}
	};

	public Scenario5() {
		super(scenario);
	}
}
