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
			StochasticProcess joinNodes = new StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(40, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
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
					eventInterArrivalTime(constant(2000));
					raise(20, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
				}
			};


            StochasticProcess addEntries1 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(2000));
                    raise(50, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess addEntries2 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(2000));
                    raise(50, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess search = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(2000));
                    raise(50, Operations.search(), uniform(0, Integer.MAX_VALUE));
                }
            };

			joinNodes.start();
			addEntries.startAfterTerminationOf(60 * 10 * 1000, joinNodes);
//			massiveJoin.startAfterTerminationOf(2000, addEntries);
//          search.startAfterTerminationOf(10000, addEntries);

            addEntries1.startAfterTerminationOf(1000000, addEntries);
            addEntries2.startAfterStartOf(100000, addEntries1);
		}
	};

	public Scenario2() {
		super(scenario);
	}
}
