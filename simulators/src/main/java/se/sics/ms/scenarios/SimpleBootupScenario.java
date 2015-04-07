package se.sics.ms.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.ms.simulation.SweepOperations;


/**
 * Simple Testing Scenario.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SimpleBootupScenario {
    
    
    public static SimulationScenario boot(final long seed){
        
        SimulationScenario scenario = new SimulationScenario() {
            
            {
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(3 , SweepOperations.startNodeCmdOperation, uniform(0,Integer.MAX_VALUE));
                    }
                };
                
                StochasticProcess addIndexEntryCommand = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(3000));
                        raise(1, SweepOperations.addIndexEntryCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };

                peerJoin.start();
//                addIndexEntryCommand.startAfterTerminationOf(10000, peerJoin);
                // === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}

