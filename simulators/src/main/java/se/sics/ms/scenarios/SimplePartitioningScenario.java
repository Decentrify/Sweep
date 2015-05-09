package se.sics.ms.scenarios;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;


/**
 * Simple Testing Scenario.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SimplePartitioningScenario {

    
    private static long depth= 2;
    private static long bucketSize = 2;

    public static SimulationScenario boot(final long seed) {
        
        SimulationScenario scenario = new SimulationScenario() {
            
            {

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startAggregatorNodeCmd);
                    }
                };

                StochasticProcess generatePartitionNodeMap = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.generatePartitionNodeMap, constant(depth), constant(bucketSize));
                    }
                };


                StochasticProcess partitionPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( (int) (Math.pow(2, depth) * bucketSize ) , SweepOperations.startPartitionNodeCmd, uniform(0,Integer.MAX_VALUE));
                    }
                };


                StochasticProcess partitionEntryAdd = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( 5 , SweepOperations.addPartitionIndexEntryCommand, uniform(0,Integer.MAX_VALUE));
                    }
                };
                
                
                
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startNodeCmdOperation, constant(Integer.MIN_VALUE));
                    }
                };
                
                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };
                
                StochasticProcess searchIndexEntry = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.searchIndexEntry, uniform(0, Integer.MAX_VALUE));
                        
                    }
                };
                
//                startAggregatorNode.start();
//                generatePartitionNodeMap.startAfterTerminationOf(10000, startAggregatorNode);
                generatePartitionNodeMap.start();
                partitionPeerJoin.startAfterTerminationOf(10000, generatePartitionNodeMap);
                partitionEntryAdd.startAfterTerminationOf(10000, partitionPeerJoin);
                
//                peerJoin.start();
//                specialPeerJoin.startAfterTerminationOf(30000, peerJoin);
//                addIndexEntryCommand.startAfterTerminationOf(30000, peerJoin);
//                searchIndexEntry.startAfterTerminationOf(50000, addIndexEntryCommand);
                // === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

