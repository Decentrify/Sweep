package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.server.util.DesignInfo;

/**
 * Designer Information for the replication lag.
 *
 * Created by babbar on 2015-09-20.
 */
public class ReplicationLagDesignInfo implements DesignInfo{

    public double averageLag;
    public int maxLag;
    public int minLag;


    public ReplicationLagDesignInfo(double averageLag, int maxLag, int minLag){

        this.averageLag = averageLag;
        this.minLag = minLag;
        this.maxLag = maxLag;
    }

}
