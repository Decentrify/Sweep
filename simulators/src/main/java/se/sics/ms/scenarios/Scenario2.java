package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 * Initializes the system with 100 nodes, add 200 entries to the index and
 * another 100 nodes after that.
 */
@SuppressWarnings("serial")
public class Scenario2 extends Scenario {
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

			StochasticProcess massiveJoin = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(100, Operations.peerJoin());
				}
			};

			StochasticProcess addEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
					raise(200, Operations.addIndexEntry(), uniform(1, 100));
				}
			};

			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
			addEntries.startAfterTerminationOf(5000, joinNodes);
			massiveJoin.startAfterTerminationOf(2000, addEntries);
		}
	};

	public Scenario2() {
		super(scenario);
	}
}
