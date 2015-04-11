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
    
    private long leaderLeaseTime;
    private long followerLeaseTime;
    private int viewSize;
    private final int convergenceRounds;
    private final double convergenceTest;
    private final int maxLeaderGroupSize;

    private ElectionConfig(long leaderLeaseTime,long followerLeaseTime, int viewSize, int convergenceRounds, double convergenceTest, int maxLeaderGroupSize) {
        
        this.leaderLeaseTime = leaderLeaseTime;
        this.followerLeaseTime = followerLeaseTime;
        this.viewSize = viewSize;
        this.convergenceRounds = convergenceRounds;
        this.convergenceTest = convergenceTest;
        this.maxLeaderGroupSize = maxLeaderGroupSize;
    }



    public int getViewSize() {
        return viewSize;
    }

    public long getLeaderLeaseTime(){
        return this.leaderLeaseTime;
    }
    
    public long getFollowerLeaseTime(){
        return this.followerLeaseTime;
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
        private long leaseTime = 60000; // 60 seconds
        private int convergenceRounds = 6;
        private double convergenceTest = 0.8d;
        private int maxLeaderGroupSize = 10;
        private long extendedLeaseTime;

        public ElectionConfigBuilder(int viewSize){
            this.viewSize = viewSize;
            this.extendedLeaseTime = this.leaseTime  + (long)(this.leaseTime * (0.2));
        }

        public ElectionConfigBuilder setLeaseTime(long leaseTime){
            this.leaseTime = leaseTime;
            this.extendedLeaseTime = this.leaseTime  + (long)(this.leaseTime * (0.2));
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
            return new ElectionConfig(this.leaseTime,this.extendedLeaseTime, this.viewSize, convergenceRounds, convergenceTest, this.maxLeaderGroupSize);
        }
    }
    
}
