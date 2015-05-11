package se.sics.ms.scenarios.special;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This scenario deals with the sharding of the system in terms of the easing the load on the system by dividing the data into separate
 * shards and keeping pointers to other shards in the system.
 * 
 * The pointer to other shards help us to search in the system and get the data faster.
 *
 * Created by babbarshaer on 2015-05-10.
 */
public class ShardingAndSearchScenario {


    public static SimulationScenario boot(final long seed, final long depth, final long bucketSize, final int throughput, final int numEntries) {


        final SimulationScenario scenario = new SimulationScenario() {

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
                        raise(1, SweepOperations.startAggregatorNodeCmd);
                    }
                };

                StochasticProcess generatePartitionNodeMap = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, SweepOperations.generatePartitionNodeMap, constant(depth), constant(bucketSize));
                    }
                };


                StochasticProcess partitionPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise((int) (Math.pow(2, depth) * bucketSize), SweepOperations.startPartitionNodeCmd, uniform(0, Integer.MAX_VALUE));
                    }
                };
                
                // =====================================
                
                Map<Integer, List<StochasticProcess>> fillLevelCommand = new HashMap<Integer, List<StochasticProcess>>();
                
                final int shardSize = 8;
                
                StochasticProcess fillBucket0 = new StochasticProcess() {{
                        eventInterArrivalTime(constant(4000));
                        raise(shardSize, SweepOperations.addBucketAwareEntry, constant(0));
                }};
                
                List<StochasticProcess> level0List = new ArrayList<StochasticProcess>();
                level0List.add(fillBucket0);
                fillLevelCommand.put(0, level0List);
                
                
                for(int level =1 ; level < depth; level++) {
                    List<StochasticProcess> spList = new ArrayList<StochasticProcess>();
                    fillLevelCommand.put(level, spList);
                    for (int j = 0; j < Math.pow(2, level) ; j++) {
                        final int bucket = j;
                        StochasticProcess bucketFill = new StochasticProcess() {
                            {
                                eventInterArrivalTime(constant(4000));
                                raise(shardSize / 2, SweepOperations.addBucketAwareEntry, constant(bucket));
                            }
                        };
                        spList.add(bucketFill);
                    }
                }

                StochasticProcess searchIndexEntry = new StochasticProcess() {
                    {
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.bucketAwareSearchEntry, constant(0), constant(3000), constant(3) , constant(expectedEntries) );

                    }
                };

                changeNetworkModel.start();
                startAggregatorNode.startAfterTerminationOf(1000, changeNetworkModel);
                generatePartitionNodeMap.startAfterTerminationOf(10000, startAggregatorNode);
                partitionPeerJoin.startAfterTerminationOf(10000, generatePartitionNodeMap);
                
               // partitionEntryAdd.startAfterTerminationOf(40000, partitionPeerJoin);

                StochasticProcess previous = partitionPeerJoin;
                for(int level = 0; level < fillLevelCommand.size(); level++) {
                    
                    List<StochasticProcess> bucketCmdList = fillLevelCommand.get(level);
                    for(StochasticProcess bucketCmd : bucketCmdList) {
                        bucketCmd.startAfterTerminationOf(150 * 1000, previous);
                    }
                    previous = bucketCmdList.get(0);
                }
                
                searchIndexEntry.startAfterTerminationOf(90000, previous);
            }

        };

        scenario.setSeed(seed);
        return scenario;
    }
}