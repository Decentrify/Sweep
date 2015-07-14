package se.sics.ms.main;

import se.sics.ms.scenarios.special.BasicConvergenceScenario;
import se.sics.ms.scenarios.special.FastConvergenceScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

/**
 * Main Test Class for working with the convergence scenarios.
 * 
 * Created by babbarshaer on 2015-05-09.
 */
public class ConvergenceMainTest {
    
    public static void main(String[] args) {
        
        if(args.length < 2){
            throw new RuntimeException("Arguments Expected: { seed, bucketSize } ");
        }

        long seed = Long.valueOf(args[0]);
        long bucketSize = Long.valueOf(args[1]);

        System.out.println(" Starting the Epoch Aware Convergence Scenario with" + " seed: " + seed + " depth: " + 0 + " bucketSize: " + bucketSize + " throughput: " + 1 + " entries: " + 1);
        FastConvergenceScenario.boot(seed, 0, bucketSize, 1, 1).simulate(LauncherComp.class);
    }

}
