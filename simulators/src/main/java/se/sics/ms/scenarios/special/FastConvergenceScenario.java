package se.sics.ms.scenarios.special;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;


/**
 * Scenario relating to test the convergence capability of the system.
 * The scenarios have certain parameters that need to be moved to the arguments.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class FastConvergenceScenario {
    
    public static SimulationScenario boot(final long seed, final long depth, final long bucketSize, final int throughput, final int numEntries) {
        
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

                StochasticProcess generatePartitionNodeMap = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.generatePartitionNodeMap, constant(depth), constant(bucketSize));
                    }
                };


                StochasticProcess partitionPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise( (int) (Math.pow(2, depth) * bucketSize ) , SweepOperations.startPartitionNodeCmd, uniform(1 , Integer.MAX_VALUE));
                    }
                };


                StochasticProcess partitionEntryAdd = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000 / throughput));
                        raise( numEntries , SweepOperations.addPartitionIndexEntryCommand, uniform(0,Integer.MAX_VALUE));
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
                        eventInterArrivalTime(constant(3000 / throughput));
                        raise(1, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };
                
                StochasticProcess searchIndexEntry = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.searchIndexEntry, uniform(0, Integer.MAX_VALUE), constant(3000), constant(3));

                    }
                };

                changeNetworkModel.start();
                startAggregatorNode.startAfterTerminationOf(1000, changeNetworkModel);
                generatePartitionNodeMap.startAfterTerminationOf(10000, startAggregatorNode);
                partitionPeerJoin.startAfterTerminationOf(10000, generatePartitionNodeMap);
                partitionEntryAdd.startAfterTerminationOf(200 * 1000, partitionPeerJoin);
                
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

