/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.scenarios.general;

import se.sics.ms.simulation.Operations;

/**
 *
 * @author alidar
 */

/**
 * Scenario to test partitioning code.
 */
@SuppressWarnings("serial")
public class GradualPartitioningScenario extends Scenario {
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
                    eventInterArrivalTime(constant(1000));
                    raise(10, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess addEntries1 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000));
                    raise(10, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess addEntries2 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000));
                    raise(10, Operations.addIndexEntry(), uniform(0, Integer.MAX_VALUE));
                }
            };

            StochasticProcess searchEntries = new StochasticProcess() {{

                eventInterArrivalTime(constant(300));
                raise(10, Operations.search(), uniform(0, Integer.MAX_VALUE));

            }};


            joinNodes.start();
            // Add Entries.
            addEntries.startAfterTerminationOf(300000, joinNodes);
            addEntries1.startAfterTerminationOf(500000, addEntries);
            addEntries2.startAfterTerminationOf(500000, addEntries1);
            // Start Searching Them.
//            searchEntries.startAfterTerminationOf(50000,addEntries2);
        }
    };

    public GradualPartitioningScenario() {
        super(scenario);
    }

}