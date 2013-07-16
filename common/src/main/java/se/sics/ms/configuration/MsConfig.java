/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.configuration;

import java.io.IOException;
import se.sics.gvod.config.VodConfig;

/**
 *
 * @author jdowling
 */
public class MsConfig extends VodConfig {

    public static final int ELECTION_INDEX_TIMEOUT = 30 * 1000;
    public static final int ELECTION_DEATH_TIMEOUT = 20 * 1000;
    public static final int ELECTION_REJECTED_TIMEOUT = 20 * 1000;
    public static final boolean ELECTION_NODE_SUGGESTION = true;
    public static final int ELECTION_VOTE_REQUEST_TIMEOUT = 20 * 1000;
    public static final int ELECTION_HEARTBEAT_WAIT_TIMEOUT = 10 * 1000;
    public static final int ELECTION_HEARTBEAT_TIMEOUTDELAY = 5 * 1000;
    ;
    public static final int ELECTION_MIN_SIZE_ELECTIONGROUP = 3;
    public static final double ELECTION_MIN_PERCENTAGE_VOTES = .5d;
    public static final int ELECTION_WAIT_FOR_NO_INDEX_MESSAGES = 10 * 1000;
    public static final int ELECTION_HEARTBEAT_TIMEOUT_INTERVAL = 30 * 1000;
    public static final int ELECTION_MIN_NUMBER_CONVERGED_NODES = 3;
    public static final double ELECTION_DEATH_VOTE_MAJORITY_PERCENTAGE = .5d;
    public static final double ELECTION_LEADER_DEATH_MAJORITY_PERCENTAGE = .5d;

    
    public static final int SEARCH_NUM_PARTITIONS = 1;
    public static final int SEARCH_MAX_NUM_ROUTING_ENTRIES = 5;
    public static final int SEARCH_MAX_EXCHANGE_COUNT = 10;
    public static final int SEARCH_QUERY_TIMEOUT = 10*1000;
    public static final int SEARCH_ADD_TIMEOUT= 30*1000;
    public static final int SEARCH_REPLICATION_TIMEOUT = 30*1000;
    public static final int SEARCH_REPLICATION_MAXIMUM = 5;
    public static final int SEARCH_REPLICATION_MINIMUM = 2; 
    public static final int SEARCH_RETRY_COUNT = 2;
    public static final int SEARCH_GAP_TIMEOUT = 10*1000;
    public static final int SEARCH_GAP_DETECTION_TTL = 20;
    public static final int SEARCH_GAP_DETECTION_TIMEOUT = 10*1000;
    public static final int SEARCH_HITS_PER_QUERY = 25;
    public static final int SEARCH_RECENT_REQUESTS_GCINTERVAL = 100;

    protected MsConfig(String[] args) throws IOException {
        super(args);
    }

    public static synchronized MsConfig init(String[] args) throws IOException {
        if (singleton != null) {
            return (MsConfig) singleton;
        }

        singleton = new MsConfig(args);
        return (MsConfig) singleton;
    }
}
