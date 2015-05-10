/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.configuration;

import se.sics.gvod.config.VodConfig;

import java.io.IOException;

/**
 *
 * @author jdowling
 */
public class MsConfig extends VodConfig {

    public static enum Categories {
        Video, Music, Books
    }

    public static final int ELECTION_DEATH_TIMEOUT = 20 * 1000;
    public static final int ELECTION_REJECTED_TIMEOUT = 20 * 1000;
    public static final int ELECTION_VOTE_REQUEST_TIMEOUT = 20 * 1000;
    public static final int ELECTION_HEARTBEAT_WAIT_TIMEOUT = 10 * 1000;
    public static final int ELECTION_HEARTBEAT_TIMEOUTDELAY = 5 * 1000;

    public static final int ELECTION_MIN_SIZE_ELECTIONGROUP = 3;
    public static final double ELECTION_MIN_PERCENTAGE_VOTES = .5d;
    public static final int ELECTION_HEARTBEAT_TIMEOUT_INTERVAL = 30 * 1000;
    public static final int ELECTION_MIN_NUMBER_CONVERGED_NODES = 3;
    public static final double ELECTION_DEATH_VOTE_MAJORITY_PERCENTAGE = .5d;
    public static final double ELECTION_LEADER_DEATH_MAJORITY_PERCENTAGE = .5d;

    public static final int SEARCH_MAX_EXCHANGE_COUNT = 10;
    public static final int SEARCH_QUERY_TIMEOUT = 10*1000;
    public static final int SEARCH_ADD_TIMEOUT= 30*1000;
    public static final int SEARCH_REPLICATION_TIMEOUT = 30*1000;
    public static final int SEARCH_RETRY_COUNT = 2;
    public static final int SEARCH_HITS_PER_QUERY = 25;
    public static final int SEARCH_RECENT_REQUESTS_GCINTERVAL = 5 * 60 * 1000;
    public static final int MAX_LEADER_ID_HISTORY_SIZE = 5;
    public static final int SEARCH_MAX_SEARCH_RESULTS = 500;

    // Index hash exchange
    public static final int SEARCH_INDEX_EXCHANGE_TIMEOUT = 5 * 1000;
    public static final int SEARCH_INDEX_EXCHANGE_REQUEST_NUMBER = 3;
    public static final int INDEX_EXCHANGE_PERIOD = 10 * 1000;

    public static final int GRADIENT_MAX_NUM_ROUTING_ENTRIES = 20;
    public static final int GRADIENT_LEADER_LOOKUP_TIMEOUT = 30 * 1000;
    public static final int GRADIENT_SEARCH_PARALLELISM = 3;
    public static final int GRADIENT_LATEST_RTT_STORE_LIMIT = 10;
    public static final double GRADIENT_RTT_ANOMALY_TOLERANCE = 2.0;

    public static final long MAX_ENTRIES_ON_PEER = 10;
    public static final int MAX_PARTITION_HISTORY_SIZE = 5;
    public static final int MAX_PARTITION_ID_LENGTH = 16;

    // Generic control exchange message.
    public static final int CONTROL_MESSAGE_EXCHANGE_PERIOD = 10*1000;
    public static final int DELAYED_PARTITIONING_REQUEST_TIMEOUT = 5*1000;
    public static final int CONTROL_MESSAGE_ENUM_SIZE = 2;

    // Two phase commit timeout.
    public static final int LEADER_GROUP_SIZE = 3;
    public static final int PARTITION_PREPARE_TIMEOUT=10*1000;
    public static final int PARTITION_COMMIT_REQUEST_TIMEOUT=5*1000;
    public static final int PARTITION_COMMIT_TIMEOUT= 5*1000;


    //overrides.
    public static final int GRADIENT_VIEW_SIZE = 5;
    public static final int GRADIENT_CONVERGENCE_TEST_ROUNDS = 8;
    public static final int GRADIENT_SHUFFLE_PERIOD = 3000;
    public static final double GRADIENT_CONVERGENCE_TEST = 0.8d;
    
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
