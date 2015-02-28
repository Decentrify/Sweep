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
                StochasticProcess startCommandCenter = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, SweepOperations.startNodeCmdOperation);
                    }
                };
                
                
                StochasticProcess peerJoin = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(3, SweepOperations.peerJoinCommand, uniform(0, Integer.MAX_VALUE));
                    }
                };
                
                
                startCommandCenter.start();
                peerJoin.startAfterTerminationOf(3000, startCommandCenter);
//                terminateAfterTerminationOf(5000, startCommandCenter);
                // === Add a termination event.
            }
        };
        
        scenario.setSeed(seed);
        
        return scenario;
    }
}
