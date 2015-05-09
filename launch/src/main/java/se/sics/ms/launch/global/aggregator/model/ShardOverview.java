package se.sics.ms.launch.global.aggregator.model;

import org.javatuples.Triplet;

/**
 * Overview of a particular shard.
 * Created by babbarshaer on 2015-05-09.
 */
public class ShardOverview {
    
    private int leaderId;
    private long numEntries;
    private long size;
    Triplet<Integer, Integer, Integer> bucketDivision;
    
    public ShardOverview(int leaderId, long numEntries, long size, Triplet<Integer, Integer, Integer> bucketDivision){
        
        this.leaderId= leaderId;
        this.numEntries = numEntries;
        this.size = size;
        this.bucketDivision = bucketDivision;
    }


    @Override
    public String toString() {
        return "ShardOverview{" +
                "leaderId=" + leaderId +
                ", numEntries=" + numEntries +
                ", size=" + size +
                ", bucketDivision (50, 75, 100)=" + bucketDivision +
                '}';
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getNumEntries() {
        return numEntries;
    }

    public long getSize() {
        return size;
    }

    public Triplet<Integer, Integer, Integer> getBucketDivision() {
        return bucketDivision;
    }
}
