/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.scenarios;

import se.sics.ms.simulation.Operations;

/**
 *
 * @author alidar
 */

/**
 * Scenario to test partitioning code.
 */
@SuppressWarnings("serial")
public class GenericControlPullMechanismScenario extends Scenario {
    private static ThreadedSimulationScenario scenario = new ThreadedSimulationScenario() {
        {
            StochasticProcess joinNodes = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(50, Operations.peerJoin(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess addEntries = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(50, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess searchEntries = new StochasticProcess() {{

                eventInterArrivalTime(constant(300));
                raise(50, Operations.search(), uniform(0, Integer.MAX_VALUE));

            }};


            joinNodes.start();
            addEntries.startAfterTerminationOf(10000, joinNodes);
        }
    };

    public GenericControlPullMechanismScenario() {
        super(scenario);
    }

}
