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

                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( 6 , SweepOperations.startNodeCmdOperation, uniform(0, Integer.MAX_VALUE) );
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
                        raise(10, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
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
                        eventInterArrivalTime(constant(8000));
                        raise(2, SweepOperations.searchIndexEntry, constant(56871960), constant(6000), constant(3));

                    }
                };

//                startAggregatorNode.start();
//                specialPeerJoin.start();
                peerJoin.start();
//                largestPeerJoin.startAfterTerminationOf(1000, peerJoin);
//                addIndexEntryCommand.startAfterTerminationOf(100000, peerJoin);
//                specialPeerJoin.startAfterTerminationOf(30000, addIndexEntryCommand);
//                specialAddEntryCommand.startAfterTerminationOf(60000, specialPeerJoin);
                
//                searchIndexEntry.startAfterTerminationOf(50000, peerJoin);
//                === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

