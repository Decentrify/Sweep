package se.sics.p2ptoolbox.election.core;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;


import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Comparator;

/**
 * Configuration Class for the Election Module.
 * Created by babbarshaer on 2015-03-27.
 */
public class ElectionConfig {
    
    private long leaseTime;
    private int viewSize;
    private final int convergenceRounds;
    private final double convergenceTest;
    private final int maxLeaderGroupSize;

    private ElectionConfig(long leaseTime, int viewSize, int convergenceRounds, double convergenceTest, int maxLeaderGroupSize) {
        this.leaseTime = leaseTime;
        this.viewSize = viewSize;
        this.convergenceRounds = convergenceRounds;
        this.convergenceTest = convergenceTest;
        this.maxLeaderGroupSize = maxLeaderGroupSize;
    }



    public int getViewSize() {
        return viewSize;
    }

    public long getLeaseTime(){
        return this.leaseTime;
    }

    public int getConvergenceRounds(){
        return this.convergenceRounds;
    }
    
    public double getConvergenceTest(){
        return this.convergenceTest;
    }

    public int getMaxLeaderGroupSize(){
        return this.maxLeaderGroupSize;
    }
    
    /**
     * Builder Class for the Election Configuration.
     * Created by babbarshaer on 2015-03-27.
     */
    public static class ElectionConfigBuilder {

        private int viewSize;
        private long leaseTime = 120000; // 120 seconds
        private int convergenceRounds = 6;
        private double convergenceTest = 0.8d;
        private int maxLeaderGroupSize = 10;

        public ElectionConfigBuilder(int viewSize){
            this.viewSize = viewSize;
        }
        
        public ElectionConfigBuilder setLeaseTime(long leaseTime){
            this.leaseTime = leaseTime;
            return this;
        }
        
        public ElectionConfigBuilder setConvergenceRounds(int convergenceRounds){
            this.convergenceRounds = convergenceRounds;
            return this;
        }
        
        public ElectionConfigBuilder setConvergenceTest(double convergenceTest){
            this.convergenceTest = convergenceTest;
            return this;
        }

        public ElectionConfigBuilder setMaxLeaderGroupSize(int maxLeaderGroupSize){
            this.maxLeaderGroupSize= maxLeaderGroupSize;
            return this;
        }        
        
        public ElectionConfig buildElectionConfig(){
            return new ElectionConfig(this.leaseTime, this.viewSize, convergenceRounds, convergenceTest, this.maxLeaderGroupSize);
        }
    }
    
}
