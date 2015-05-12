package se.sics.ms.main;

import se.sics.ms.scenarios.special.BasicFLashCrowdScenario;
import se.sics.ms.scenarios.special.FastConvergenceScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

/**
 * Main Test Class for working with the convergence scenarios.
 * 
 * Created by babbarshaer on 2015-05-09.
 */
public class FlashCrowdMainTest {
    
    public static void main(String[] args) {
        
        if(args.length < 5){
            throw new RuntimeException("Arguments Expected: { seed, throughput, entries to add, initialClusterSize, flashCrowdSize }");
        }

        long seed = Long.valueOf(args[0]);
        int throughput = Integer.valueOf(args[1]);
        int numEntries = Integer.valueOf(args[2]);
        int initialClusterSize = Integer.valueOf(args[3]);
        int flashCrowdSize = Integer.valueOf(args[4]);
        
        System.out.println(" Starting the Flash Crowd Test with" + " seed: " + seed + " throughput: " + throughput + " entries: " + numEntries + " initialClusterSize: " + initialClusterSize + " flashCrowdSize" + flashCrowdSize);
        BasicFLashCrowdScenario.boot(seed,throughput, numEntries, initialClusterSize, flashCrowdSize).simulate(LauncherComp.class);
    }

}
