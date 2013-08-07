package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 * Scenario adding real magnet links.
 */
@SuppressWarnings("serial")
public class Scenario5 extends Scenario {
	private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
		{
			StochasticProcess startUp = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(1, Operations.peerJoin(), uniform(0, 0));
				}
			};

			StochasticProcess joinNodes = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(99, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
				}
			};

			StochasticProcess addMagnetEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
					raise(100, Operations.addMagnetEntry(), uniform(0, Integer.MAX_VALUE));
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
