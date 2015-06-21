package se.sics.ms.scenarios.special;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;

/**
 * Scenario for generating the flash crowd in the system.
 * Flash Crowd means that once the system has stabalized in terms of nodes have added entries, send in a burst
 * of new nodes joining the system.
 *
 * The
 * Created by babbarshaer on 2015-05-10.
 */
public class BasicAvailabilityScenario {


    public static SimulationScenario boot(final long seed, final int throughput, final int initialClusterSize,  final int numEntries, final int time, final int entryChangePerSec) {



        SimulationScenario scenario = new SimulationScenario() {

            {

                StochasticProcess changeNetworkModel = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(300));
                        raise(1, SweepOperations.uniformNetworkModel);
                    }
                };

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startAggregatorNodeCmd);
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(7 , SweepOperations.startLeaderGroupNodes, constant(Integer.MIN_VALUE));
                    }
                };


                StochasticProcess initialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( (initialClusterSize-7) , SweepOperations.startNodeCmdOperation, uniform(1 , Integer.MAX_VALUE));
                    }
                };

                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000 / throughput));
                        raise( numEntries , SweepOperations.addIndexEntryCommand, constant(Integer.MIN_VALUE));
                    }
                };



                StochasticProcess churnEntryAddition = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000 / entryChangePerSec));
                        raise( time* entryChangePerSec , SweepOperations.addIndexEntryCommand, constant(Integer.MIN_VALUE));
                    }
                };

                changeNetworkModel.start();
                startAggregatorNode.startAfterTerminationOf(1000, changeNetworkModel);
                specialPeerJoin.startAfterTerminationOf(10000, startAggregatorNode);
                initialPeerJoin.startAfterTerminationOf(5000, specialPeerJoin);
                addIndexEntryCommand.startAfterTerminationOf(50000, initialPeerJoin);

                // Churn Scenario Commands.
//                churnPeerJoin.startAfterTerminationOf(150000, addIndexEntryCommand);
//                churnPeerKillProcess.startAtSameTimeWith(churnPeerJoin);
                churnEntryAddition.startAfterTerminationOf(50*1000, addIndexEntryCommand);

            }
        };

        scenario.setSeed(seed);

        return scenario;
    }



}
