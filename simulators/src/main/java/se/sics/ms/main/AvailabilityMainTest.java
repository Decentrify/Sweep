package se.sics.ms.main;

import se.sics.ms.scenarios.special.BasicAvailabilityScenario;
import se.sics.ms.scenarios.special.BasicChurnScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

/**
 * Main Test Class for working with the convergence scenarios.
 * 
 * Created by babbarshaer on 2015-05-09.
 */
public class AvailabilityMainTest {
    
    public static void main(String[] args) {
        
        if(args.length < 6){
            throw new RuntimeException("Arguments Expected: { seed, throughput, initialClusterSize, numEntries, time, entryChangePerSecond }");
        }

        long seed = Long.valueOf(args[0]);
        int throughput = Integer.valueOf(args[1]);
        int initialClusterSize = Integer.valueOf(args[2]);
        int numEntries = Integer.valueOf(args[3]);
        int time = Integer.valueOf(args[4]);
        int entryChangePerSecond = Integer.valueOf(args[5]);

        

        System.out.println(" Starting the Epoch Aware Availability Scenario with"
                + " seed: " + seed 
                + " throughput: " + throughput 
                + " initialClusterSize: "+ initialClusterSize 
                + " entries: " + numEntries
                + " time: " + time
                + " entryChangePerSecond: "  + entryChangePerSecond);

        BasicAvailabilityScenario.boot(seed, throughput, initialClusterSize, numEntries, time, entryChangePerSecond).simulate(LauncherComp.class);
    }

}
