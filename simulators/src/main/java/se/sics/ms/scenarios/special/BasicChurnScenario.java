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
public class BasicChurnScenario {


    public static SimulationScenario boot(final long seed, final int throughput, final int initialClusterSize,  final int numEntries, final int churnPeerAdd, final int churnPeerKill, final int churnEntryJoin) {

        SimulationScenario scenario = new SimulationScenario() {

            {

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startAggregatorNodeCmd);
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startNodeCmdOperation, constant(Integer.MIN_VALUE));
                    }
                };
                
                
                StochasticProcess initialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(initialClusterSize , SweepOperations.startNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };

                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000 / throughput));
                        raise( 1 , SweepOperations.addIndexEntryCommand, constant(Integer.MIN_VALUE));
                    }
                };
                
                

                StochasticProcess churnPeerKillProcess = new StochasticProcess() {
                    {
                        System.out.println(" Initiating Killing of Nodes as part of churn ... ");
                        eventInterArrivalTime(constant(1000));
                        raise(churnPeerKill , SweepOperations.killNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };

                StochasticProcess churnPeerJoin = new StochasticProcess() {
                    {
                        System.out.println(" Initiating the peer join as part of churn ... ");
                        eventInterArrivalTime(constant(1000));
                        raise(churnPeerAdd , SweepOperations.startNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };

                StochasticProcess churnPeerEntryJoin = new StochasticProcess() {
                    {
                        System.out.println(" Initiating the entry addition as part of churn ... ");
                        eventInterArrivalTime(constant(1000));
                        raise(churnPeerKill , SweepOperations.addIndexEntryCommand, uniform(0,Integer.MAX_VALUE));
                    }
                };
                
                
                
                StochasticProcess churnEntryAddition = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000 / throughput));
                        raise( churnEntryJoin , SweepOperations.addIndexEntryCommand, constant(Integer.MIN_VALUE));
                    }
                };

                startAggregatorNode.start();
                specialPeerJoin.startAfterTerminationOf(10000, startAggregatorNode);
                initialPeerJoin.startAfterTerminationOf(5000, specialPeerJoin);
                addIndexEntryCommand.startAfterTerminationOf(40000, initialPeerJoin);

                // Churn Scenario Commands.
                churnPeerJoin.startAfterTerminationOf(150000, addIndexEntryCommand);
                churnPeerKillProcess.startAtSameTimeWith(churnPeerJoin);
                churnPeerEntryJoin.startAtSameTimeWith(churnPeerJoin);

            }
        };

        scenario.setSeed(seed);

        return scenario;
    }
    
    
    
}
