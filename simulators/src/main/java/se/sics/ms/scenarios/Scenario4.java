package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 * Initializes the system with 100 nodes, add 200 entries to the index and
 * another 100 nodes after that. During the adding process the leader is
 * crashed. Hence, some add operations might be lost. Later, another 100 nodes
 * are joined and churn is produced. After the churn was introduced, it might
 * take some time until the newly added nodes received all indexes.
 */
@SuppressWarnings("serial")
public class Scenario4 extends Scenario {
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

			StochasticProcess massiveJoin = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(100, Operations.peerJoin());
				}
			};

			StochasticProcess churn = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(2000));
					raise(30, Operations.peerJoin());
					raise(30, Operations.peerFail(), uniform(30, 200));
				}
			};

			StochasticProcess failLeader = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(1000));
					raise(1, Operations.peerFail(), uniform(0, 0));
				}
			};

			StochasticProcess addEntries = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(500));
					raise(200, Operations.addIndexEntry(), uniform(0, 200));
				}
			};

			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
			addEntries.startAfterTerminationOf(5000, joinNodes);
			failLeader.startAfterTerminationOf(7000, joinNodes);
			massiveJoin.startAfterTerminationOf(2000, addEntries);
			churn.startAfterTerminationOf(30000, massiveJoin);
		}
	};

	public Scenario4() {
		super(scenario);
	}
}
