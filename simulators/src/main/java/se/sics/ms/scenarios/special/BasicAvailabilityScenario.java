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

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(300));
                        raise(1, SweepOperations.startAggregatorNode);
                    }
                };


                StochasticProcess startCaracalClient = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startCaracalClient, uniform(0, Integer.MAX_VALUE));
                    }
                };


                StochasticProcess changeNetworkModel = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(300));
                        raise(1, SweepOperations.uniformNetworkModel);
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
                        System.out.println("Starting the main churn entry addition scenario");
                        eventInterArrivalTime(constant(1000 / entryChangePerSec));
                        raise( time* entryChangePerSec , SweepOperations.addIndexEntryCommand, constant(Integer.MIN_VALUE));
                    }
                };

                startAggregatorNode.start();
                startCaracalClient.startAfterTerminationOf(5000, startAggregatorNode);
                specialPeerJoin.startAfterTerminationOf(10000, startCaracalClient);
                initialPeerJoin.startAfterTerminationOf(5000, specialPeerJoin);
                addIndexEntryCommand.startAfterTerminationOf(50000, initialPeerJoin);
                churnEntryAddition.startAfterTerminationOf(50*1000, addIndexEntryCommand);

            }
        };

        scenario.setSeed(seed);

        return scenario;
    }



}
