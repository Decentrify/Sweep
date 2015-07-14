package se.sics.ms.scenarios.general;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;


/**
 * Simple testing scenario in which a very good node is
 * introduced in the system at a very late time.
 *
 * The node catches up to other nodes and then try to
 * assert itself as the leader to the other nodes.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SimpleCatchUpAndBackOffScenario {


    public static SimulationScenario boot(final long seed) {
        
        SimulationScenario scenario = new SimulationScenario() {
            
            {
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(5 , SweepOperations.startPAGNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startPAGNodeCmdOperation, constant(Integer.MIN_VALUE));
                    }
                };
                
                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(5000));
                        raise(25 , SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
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
                        raise(1, SweepOperations.searchIndexEntry, uniform(0, Integer.MAX_VALUE), constant(3000), constant(3));

                    }
                };

                peerJoin.start();
                addIndexEntryCommand.startAfterTerminationOf(30000, peerJoin);
                specialPeerJoin.startAfterTerminationOf(30000, addIndexEntryCommand); // Start a catch up node.
//                specialAddEntryCommand.startAfterTerminationOf(60000, specialPeerJoin);
                
//                searchIndexEntry.startAfterTerminationOf(50000, addIndexEntryCommand);
                // === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

