package se.sics.ms.main;

import se.sics.ms.scenarios.special.ShardingAndSearchScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

/**
 * Main Test Class for working with the convergence scenarios.
 * 
 * Created by babbarshaer on 2015-05-09.
 */
public class ShardAndSearchTest {
    
    public static void main(String[] args) {
        
        if(args.length < 8) {
            throw new RuntimeException("Arguments Expected: { seed, depth, bucketSize, throughput, shardSize, searchTimeout, fanout , searches}");
        }

        long seed = Long.valueOf(args[0]);
        long depth = Long.valueOf(args[1]);
        long bucketSize = Long.valueOf(args[2]);
        int throughput = Integer.valueOf(args[3]);
        int numEntries = Integer.valueOf(args[4]);
        int searchTimeout = Integer.valueOf(args[5]);
        int fanout = Integer.valueOf(args[6]);
        int searches = Integer.valueOf(args[7]);

        System.out.println(" Starting the Search and Shard Scenario with" + " seed: " + seed + " depth: " + depth + " bucketSize: " + bucketSize + " throughput: " + throughput + " entries: " + numEntries +" searchTimeout: " + searchTimeout +" fanout: " + fanout + " searches: " + searches) ;
        ShardingAndSearchScenario.boot(seed, depth, bucketSize, throughput, numEntries,searchTimeout,fanout, searches).simulate(LauncherComp.class);
    }

}
