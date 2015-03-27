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
    
    private ElectionConfig (long leaseTime, Comparator<PeerView> utilityComparator, int viewSize, PublicKey publicKey, PrivateKey privateKey) {
        this.leaseTime = leaseTime;
        this.utilityComparator = utilityComparator;
        this.viewSize = viewSize;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
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

        public ElectionConfig buildElectionConfig(){
            return new ElectionConfig(this.leaseTime, this.utilityComparator, this.viewSize, publicKey, privateKey);
        }
    }
    
}
