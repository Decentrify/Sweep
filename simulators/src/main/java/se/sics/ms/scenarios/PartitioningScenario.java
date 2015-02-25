/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.ms.simulation.Operations;

/**
 *
 * @author alidar
 */

/**
 * Scenario to test partitioning code.
 */
@SuppressWarnings("serial")
public class PartitioningScenario extends Scenario {
    private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
        {
            SimulationScenario.StochasticProcess joinNodes = new SimulationScenario.StochasticProcess() {
                {
                    eventInterArrivalTime(constant(3000));
                    raise(10, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));


                }
            };
            StochasticProcess addEntries = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(2000));
                    raise(15, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));


                }
            };

            StochasticProcess searchEntries = new StochasticProcess() {{

                eventInterArrivalTime(constant(300));
                raise(1, Operations.search(), uniform(0, Integer.MAX_VALUE));

            }};


            joinNodes.start();
            addEntries.startAfterTerminationOf(10000, joinNodes);
//            searchEntries.startAfterTerminationOf(50000, addEntries);
//            terminateProcess.startAfterTerminationOf(5000, addEntries);
        }
    };

    public PartitioningScenario() {
        super(scenario);
    }

}
