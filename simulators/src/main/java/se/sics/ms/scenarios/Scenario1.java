package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

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
					raise(9, Operations.peerJoin());
				}
			};
			
//                        StochasticProcess terminate = new StochasticProcess() {
//				{
//					eventInterArrivalTime(constant(100));
//					raise(1, Operations.terminate());
//				}
//			};
			
			startUp.start();
			joinNodes.startAfterTerminationOf(2000, startUp);
//			terminate.startAfterTerminationOf(20*1000, joinNodes);
		}
	};

	public Scenario1() {
		super(scenario);
	}
}
