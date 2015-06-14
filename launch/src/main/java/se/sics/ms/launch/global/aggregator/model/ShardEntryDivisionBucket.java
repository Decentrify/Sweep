package se.sics.ms.launch.global.aggregator.model;

import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.launch.global.aggregator.helper.DataAnalyzer;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps track of the entries in a system and also about the 
 *  
 * Created by babbarshaer on 2015-05-09.
 */
public class ShardEntryDivisionBucket {
    
    
    private int partitionId;
    private int partitionDepth;
    private Integer leaderId;
    private Long leaderEntries;
    private List<Long> bucketEntryList;
    
    private Logger logger = LoggerFactory.getLogger(DataAnalyzer.class);
    
    public ShardEntryDivisionBucket (int partitionId, int partitionDepth){
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.bucketEntryList = new ArrayList<Long>();
    }

    
    
    public void addStateToBucket(SimpleDataModel model){
        
        if(model.getPartitionId() != partitionId && model.getPartitionDepth() != partitionDepth){
            return;
        }
        
        if(model.isLeader()){
            
            logger.info("Adding Leader for the partition id :{} and partition depth: {}", partitionId, partitionDepth);
            this.leaderId = model.getNodeId();
            this.leaderEntries = model.getNumberOfEntries();
        }
        
        bucketEntryList.add(model.getNumberOfEntries());
    }
    
    public ShardOverview calculateBucketDivision(){
        
        int fiftyAndGreater = 0;
        int seventyFiveAndGreater = 0;
        int ninetyNineAndGreater = 0;
        
        
        if( leaderId == null ){
            return null;
        }

        ShardOverview shardOverview;
        
        if(leaderEntries > 0){
            
            for(Long entries : bucketEntryList){
                
                double fraction = entries / (double) leaderEntries;
                
                if(fraction >= 0.5){
                    fiftyAndGreater++;
                }
                
                if(fraction >= 0.75){
                    seventyFiveAndGreater++;
                }
                
                if(fraction >= 0.99){
                    ninetyNineAndGreater++;
                }
            }
            
            shardOverview = new ShardOverview(leaderId, leaderEntries, bucketEntryList.size(), 
                    Triplet.with(fiftyAndGreater, seventyFiveAndGreater, ninetyNineAndGreater));
        }
        
        else{
            int bucketSize = bucketEntryList.size();
            shardOverview = new ShardOverview(leaderId, leaderEntries, bucketSize, Triplet.with(bucketSize,bucketSize,bucketSize));
        }
    
        return shardOverview;
    }
}
