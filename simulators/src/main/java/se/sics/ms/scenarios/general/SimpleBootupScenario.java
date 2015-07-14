package se.sics.ms.scenarios.general;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;


/**
 * Simple Testing Scenario.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SimpleBootupScenario {


    public static SimulationScenario boot(final long seed) {
        
        SimulationScenario scenario = new SimulationScenario() {
            
            {

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startAggregatorNodeCmd);
                    }
                };
                
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( 30 , SweepOperations.startNodeCmdOperation, uniform(0, Integer.MAX_VALUE) );
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startNodeCmdOperation, constant(Integer.MIN_VALUE));
                    }
                };


                StochasticProcess largestPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startNodeCmdOperation, constant(Integer.MAX_VALUE));
                    }
                };



                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(35, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };

                StochasticProcess specialAddEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };

                StochasticProcess searchIndexEntry = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.searchIndexEntry, uniform(0, Integer.MAX_VALUE), constant(5000), constant(3));

                    }
                };

//                startAggregatorNode.start();
//                specialPeerJoin.start();
                peerJoin.start();
//                largestPeerJoin.startAfterTerminationOf(1000, peerJoin);
                addIndexEntryCommand.startAfterTerminationOf(100000, peerJoin);
//                specialPeerJoin.startAfterTerminationOf(30000, addIndexEntryCommand);
//                specialAddEntryCommand.startAfterTerminationOf(60000, specialPeerJoin);
                
                searchIndexEntry.startAfterTerminationOf(50000, addIndexEntryCommand);
                // === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

