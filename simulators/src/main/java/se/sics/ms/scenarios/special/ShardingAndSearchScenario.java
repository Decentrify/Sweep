package se.sics.ms.scenarios.special;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;

import java.util.*;

/**
 * This scenario deals with the sharding of the system in terms of the easing the load on the system by dividing the data into separate
 * shards and keeping pointers to other shards in the system.
 * 
 * The pointer to other shards help us to search in the system and get the data faster.
 *
 * Created by babbarshaer on 2015-05-10.
 */
public class ShardingAndSearchScenario {


    public static SimulationScenario boot(final long seed, final long depth, final long bucketSize, final int throughput, final int numEntries, final int searchTimeout, final int fanoutParam, final int searchRequests) {


        final SimulationScenario scenario = new SimulationScenario() {

            {
                
                
                StochasticProcess changeNetworkModel = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(300));
                        raise(1, SweepOperations.uniformNetworkModel);
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
                
                final int shardSize = numEntries;
                
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
                
                
                final Random random = new Random(seed);
                StochasticProcess searchIndexEntry250 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(250), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };


                StochasticProcess searchIndexEntry300 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(300), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };


                StochasticProcess searchIndexEntry350 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(350), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };


                StochasticProcess searchIndexEntry400 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(400), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };

                StochasticProcess searchIndexEntry450 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(450), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };


                StochasticProcess searchIndexEntry500 = new StochasticProcess() {
                    {
                        int bucketId = random.nextInt((int)Math.pow(2,depth));
                        long expectedEntries = (depth == 0 ? shardSize : (shardSize/2) * (int)Math.pow(2, depth));
                        eventInterArrivalTime(constant(8000));
                        raise( searchRequests , SweepOperations.bucketAwareSearchEntry, constant(bucketId), constant(500), constant(fanoutParam) , constant(expectedEntries) );

                    }
                };

                changeNetworkModel.start();
                generatePartitionNodeMap.startAfterTerminationOf(10000, changeNetworkModel);
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
                
                searchIndexEntry250.startAfterTerminationOf(90000, previous);
                searchIndexEntry300.startAfterTerminationOf(5000, searchIndexEntry250);
                searchIndexEntry350.startAfterTerminationOf(5000, searchIndexEntry300);
                searchIndexEntry400.startAfterTerminationOf(5000, searchIndexEntry350);
                searchIndexEntry450.startAfterTerminationOf(5000, searchIndexEntry400);
                searchIndexEntry500.startAfterTerminationOf(5000, searchIndexEntry450);
            }

        };

        scenario.setSeed(seed);
        return scenario;
    }
}