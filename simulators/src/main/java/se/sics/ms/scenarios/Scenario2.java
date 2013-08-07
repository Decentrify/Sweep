package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 * Initializes the system with 100 nodes, add 200 entries to the index and
 * another 100 nodes after that.
 */
@SuppressWarnings("serial")
public class Scenario2 extends Scenario {
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

			StochasticProcess massiveJoin = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(100, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
				}
			};

			StochasticProcess addEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
//					raise(200, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                    raise(200, Operations.addIndexEntry(), uniform(1, 100));
				}
			};

			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
			addEntries.startAfterTerminationOf(5000, joinNodes);
			massiveJoin.startAfterTerminationOf(200, addEntries);
		}
	};

	public Scenario2() {
		super(scenario);
	}
}
