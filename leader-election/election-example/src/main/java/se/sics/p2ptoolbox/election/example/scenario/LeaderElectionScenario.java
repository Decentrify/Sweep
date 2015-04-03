package se.sics.p2ptoolbox.election.example.scenario;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.p2ptoolbox.election.example.simulator.LeaderElectionOperations;

/**
 * Basic Simple Scenario Used to Test Leader Election.
 * Created by babbar on 2015-04-01.
 */
public class LeaderElectionScenario {


    public static SimulationScenario boot(final long seed) {

        SimulationScenario scenario = new SimulationScenario() {

            {
                StochasticProcess startHostManager = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(3, LeaderElectionOperations.startHostManager, uniform(0, Integer.MAX_VALUE));
                    }
                };

                StochasticProcess updatePeers = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(3, LeaderElectionOperations.updatePeersAddress);
                    }
                };

                StochasticProcess startTrueLeader = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, LeaderElectionOperations.startHostManager, uniform(0, Integer.MAX_VALUE));
                    }
                };


                StochasticProcess updatePeersAgain = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(4, LeaderElectionOperations.updatePeersAddress);
                    }
                };

                startHostManager.start();
                updatePeers.startAfterTerminationOf(10000, startHostManager);
                startTrueLeader.startAfterTerminationOf(30000, updatePeers);
                updatePeersAgain.startAfterTerminationOf(10000, startTrueLeader);
            }
        };

        scenario.setSeed(seed);

        return scenario;
    }
}
