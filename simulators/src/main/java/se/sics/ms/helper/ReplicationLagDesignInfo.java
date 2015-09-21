package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.server.util.DesignInfo;

/**
 * Designer Information for the replication lag.
 *
 * Created by babbar on 2015-09-20.
 */
public class ReplicationLagDesignInfo implements DesignInfo{

    public long time;
    public double averageLag;
    public long maxLag;
    public long minLag;


    public ReplicationLagDesignInfo(long time, double averageLag, long maxLag, long minLag){

        this.time = time;
        this.averageLag = averageLag;
        this.minLag = minLag;
        this.maxLag = maxLag;
    }

    @Override
    public String toString() {
        return "ReplicationLagDesignInfo{" +
                "time=" + time +
                ", averageLag=" + averageLag +
                ", maxLag=" + maxLag +
                ", minLag=" + minLag +
                '}';
    }
}
