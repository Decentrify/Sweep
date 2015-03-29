package se.sics.p2ptoolbox.election.core;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;


import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Comparator;

/**
 * Configuration Class for the Election Module.
 * Created by babbarshaer on 2015-03-27.
 */
public class ElectionConfig {
    
    private long leaseTime;
    private Comparator<PeerView> utilityComparator;
    private int viewSize;
    private final PublicKey publicKey;
    private final PrivateKey privateKey;
    private final int convergenceRounds;
    private final double convergenceTest;
    private final long voteTimeout;
    private final int maxLeaderGroupSize;
    
    private ElectionConfig(long leaseTime, Comparator<PeerView> utilityComparator, int viewSize, PublicKey publicKey, PrivateKey privateKey, int convergenceRounds, double convergenceTest, long voteTimeout, int maxLeaderGroupSize) {
        this.leaseTime = leaseTime;
        this.utilityComparator = utilityComparator;
        this.viewSize = viewSize;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.convergenceRounds = convergenceRounds;
        this.convergenceTest = convergenceTest;
        this.voteTimeout = voteTimeout;
        this.maxLeaderGroupSize = maxLeaderGroupSize;
    }


    public Comparator<PeerView> getUtilityComparator() {
        return utilityComparator;
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
    
    public long getVoteTimeout(){
        return this.voteTimeout;
    }
    
    public int getMaxLeaderGroupSize(){
        return this.maxLeaderGroupSize;
    }
    
    public PublicKey getPublicKey(){
        return this.publicKey;
    }
    
    /**
     * Builder Class for the Election Configuration.
     * Created by babbarshaer on 2015-03-27.
     */
    public class ElectionConfigBuilder {

        private int viewSize;
        private Comparator<PeerView> utilityComparator;
        private PublicKey publicKey;
        private PrivateKey privateKey;
        private long leaseTime = 120000; // 120 seconds
        private int convergenceRounds = 6;
        private double convergenceTest = 0.8d;
        private long voteTimeout = 20000;
        private int maxLeaderGroupSize = 10;

        public ElectionConfigBuilder(Comparator<PeerView> utilityComparator, int viewSize, PublicKey publicKey, PrivateKey privateKey){
            this.utilityComparator = utilityComparator;
            this.viewSize = viewSize;
            this.publicKey = publicKey;
            this.privateKey = privateKey;
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
        
        public ElectionConfigBuilder setVoteTimeout(long voteTimeout){
            this.voteTimeout = voteTimeout;
            return this;
        }

        public ElectionConfigBuilder setMaxLeaderGroupSize(int maxLeaderGroupSize){
            this.maxLeaderGroupSize= maxLeaderGroupSize;
            return this;
        }        
        
        public ElectionConfig buildElectionConfig(){
            return new ElectionConfig(this.leaseTime, this.utilityComparator, this.viewSize, publicKey, privateKey, convergenceRounds, convergenceTest, voteTimeout, this.maxLeaderGroupSize);
        }
    }
    
}
