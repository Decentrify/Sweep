package se.sics.ms.scenarios.general;

import se.sics.ms.simulation.SweepOperations;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;


/**
 * Simple Testing Scenario.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SimulationHostBootupScenario {


    public static SimulationScenario boot(final long seed) {
        
        SimulationScenario scenario = new SimulationScenario() {
            
            {

                StochasticProcess startAggregatorNode = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, SweepOperations.startAggregatorNode);
                    }
                };


                StochasticProcess startCaracalClient = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startCaracalClient, uniform(0, Integer.MAX_VALUE));
                    }
                };


                
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(2000));
                        raise( 12 , SweepOperations.startSimulationHostComp, uniform(0, Integer.MAX_VALUE) );
                    }
                };


                StochasticProcess specialPeerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1 , SweepOperations.startSimulationHostComp, constant(Integer.MIN_VALUE));
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

                startAggregatorNode.start();
                startCaracalClient.startAfterTerminationOf(5000, startAggregatorNode);
                peerJoin.startAfterTerminationOf(5000, startCaracalClient);
                addIndexEntryCommand.startAfterTerminationOf(60000, peerJoin);
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

