/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.configuration;

/**
 *
 * @author jdowling
 */
public class MsConfig {

    public static enum Categories {
        Video, Music, Books, Default
    }

    public static final int SEARCH_MAX_EXCHANGE_COUNT = 25;
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
    public static final int INDEX_EXCHANGE_PERIOD = 4 * 1000;

    public static final int GRADIENT_MAX_NUM_ROUTING_ENTRIES = 20;
    public static final int GRADIENT_LEADER_LOOKUP_TIMEOUT = 30 * 1000;
    public static final int GRADIENT_SEARCH_PARALLELISM = 3;
    public static final int GRADIENT_LATEST_RTT_STORE_LIMIT = 10;
    public static final double GRADIENT_RTT_ANOMALY_TOLERANCE = 2.0;

    public static final long MAX_ENTRIES_ON_PEER = 10000;
    public static final int MAX_PARTITION_HISTORY_SIZE = 5;
    public static final int MAX_PARTITION_ID_LENGTH = 16;

    // Generic control exchange message.
    public static final int CONTROL_MESSAGE_EXCHANGE_PERIOD = 3 * 1000;
    public static final int DELAYED_PARTITIONING_REQUEST_TIMEOUT = 5 * 1000;
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
    

    // PAGINATION.
    public static final int MAX_SEARCH_ENTRIES = 1000;
    public static final int DEFAULT_ENTRIES_PER_PAGE = 10;
    public static final long SCORE_DATA_CACHE_TIMEOUT = 25000;

    // Epoch Mechanism.
    public static final int MAX_EPOCH_UPDATES = 10;
    public static final int MAX_EPOCH_CONTAINER_ENTRIES = 100;            // EPOCH CONTAINER SIZE.


    // Missing.
    public static final int DEFAULT_RTO = 1 * 1000;
    public static final int GRADIENT_SHUFFLE_LENGTH = 10;
    public static final int GRADIENT_UTILITY_THRESHOLD = 10;
    public static final int GRADIENT_NUM_FINGERS = 5;
    public static final double GRADIENT_TEMPERATURE = 0.9d;
    public static final int GRADIENT_SEARCH_TIMEOUT = DEFAULT_RTO * 6;
    public static final int GRADIENT_NUM_PARALLEL_SEARCHES = 5;
    public static final int GRADIENT_SEARCH_TTL = 5;
    public static final int GRADIENT_SHUFFLE_TIMEOUT = DEFAULT_RTO;

    // Caracal Service Identifier.
    public static final int CROUPIER_SERVICE = 1;
    public static final long CARACAL_TIMEOUT = 2000;

    // Aggregator Timeout.
    public static final long AGGREGATOR_TIMEOUT = 5000;

    // Overlay Identifiers.
    public static final int CROUPIER_OVERLAY_ID = 1;
    public static final int GRADIENT_OVERLAY_ID = 2;
    public static final int T_GRADIENT_OVERLAY_ID = 3;

    public static final String SIMULATION_FILE_LOC = "/home/babbar/Documents/Experiments/Result/sweepSimResult.txt";
    public static final int SIM_SERIALIZER_START = 0;
}
