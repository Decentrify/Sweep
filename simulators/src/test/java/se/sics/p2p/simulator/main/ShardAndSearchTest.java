package se.sics.p2p.simulator.main;

import se.sics.ms.scenarios.special.ShardingAndSearchScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

/**
 * Main Test Class for working with the convergence scenarios.
 * 
 * Created by babbarshaer on 2015-05-09.
 */
public class ShardAndSearchTest {
    
    public static void main(String[] args) {
        
        if(args.length < 5) {
            throw new RuntimeException("Arguments Expected: { seed, depth, bucketSize, throughput, shardSize, searchTimeout, fanout }");
        }

        long seed = Long.valueOf(args[0]);
        long depth = Long.valueOf(args[1]);
        long bucketSize = Long.valueOf(args[2]);
        int throughput = Integer.valueOf(args[3]);
        int numEntries = Integer.valueOf(args[4]);
        int searchTimeout = Integer.valueOf(args[5]);
        int fanout = Integer.valueOf(args[6]);


        System.out.println(" Starting the Convergence Scenario with" + " seed: " + seed + " depth: " + depth + " bucketSize: " + bucketSize + " throughput: " + throughput + " entries: " + numEntries +" searchTimeout: " + searchTimeout +" fanout: " + fanout);
        ShardingAndSearchScenario.boot(seed, depth, bucketSize, throughput, numEntries,searchTimeout,fanout).simulate(LauncherComp.class);
    }

}
