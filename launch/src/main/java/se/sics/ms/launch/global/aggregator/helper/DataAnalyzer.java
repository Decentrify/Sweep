package se.sics.ms.launch.global.aggregator.helper;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.launch.global.aggregator.model.ShardEntryDivisionBucket;
import se.sics.ms.launch.global.aggregator.model.ShardOverview;
import se.sics.ms.launch.global.aggregator.model.SimpleDataModel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Analyzer for the   
 * Created by babbarshaer on 2015-05-09.
 */
public class DataAnalyzer {

    private static Logger logger = LoggerFactory.getLogger(DataAnalyzer.class);
    private static Map<Pair<Integer, Integer>, ShardEntryDivisionBucket> filteredInfoMap = 
            new HashMap<Pair<Integer, Integer>, ShardEntryDivisionBucket>();

    /**
     * Construct the buckets for the shard information objects.
     * @param dataModelCollection collection
     */
    public static String constructShardInfoBuckets(Collection<SimpleDataModel> dataModelCollection){
        
        filteredInfoMap.clear();
        
        for(SimpleDataModel dataModel : dataModelCollection){
            
            Pair<Integer, Integer> partitionIdDepthPair = Pair.with(dataModel.getPartitionId(), dataModel.getPartitionDepth());
            ShardEntryDivisionBucket entryDivisionBucket = filteredInfoMap.get(partitionIdDepthPair);
                    
            if(entryDivisionBucket == null){
                
                entryDivisionBucket = new ShardEntryDivisionBucket( 
                        partitionIdDepthPair.getValue0(), 
                        partitionIdDepthPair.getValue1());
                
                filteredInfoMap.put(partitionIdDepthPair, entryDivisionBucket); 
            }
            
            entryDivisionBucket.addStateToBucket(dataModel);
        }
        
        
        StringBuilder builder = new StringBuilder();
        
        for(Map.Entry<Pair<Integer, Integer>, ShardEntryDivisionBucket> entry : filteredInfoMap.entrySet()){
            builder.append(generateCSVString(entry)).append("\n");
        }
        
        return builder.toString();
    }
    
    
    private static String generateShardOverviewString(Map.Entry<Pair<Integer, Integer>, ShardEntryDivisionBucket> entry){
        
        StringBuilder builder = new StringBuilder();
        
        builder.append("\n");
        builder.append("=================================\n");
        builder.append(" TIME(ms): ").append(System.currentTimeMillis()).append("\n");
        builder.append("=================================\n");

        builder.append("Partition Id :").append(entry.getKey().getValue0()).append(" ").append("Partition Depth: " ).append(entry.getKey().getValue1()).append("\n");
        ShardOverview overview = entry.getValue().calculateBucketDivision();

        builder.append("\n");

        if(overview == null){
            builder.append("No Info as leader not elected.\n");
        }
        else{
            builder.append(overview.toString()).append("\n");
        }

        builder.append("=================================\n");
        builder.append("=================================\n");
        builder.append("\n");
        
        return builder.toString();
    }
    
    
    
    
    private static String generateCSVString(Map.Entry<Pair<Integer, Integer>, ShardEntryDivisionBucket> entry){
        
        StringBuilder builder = new StringBuilder();
        ShardOverview overview = entry.getValue().calculateBucketDivision();
        
        if(overview == null){
            return builder.append(entry.getKey().getValue0()).append(",")
                    .append(entry.getKey().getValue1()).append(",").toString();
        }
        
        
        builder.append(entry.getKey().getValue0()).append(",")
                .append(entry.getKey().getValue1()).append(",")
                .append(System.currentTimeMillis()).append(",")
                .append(overview.getNumEntries()).append(",")
                .append(overview.getSize()).append(",")
                .append(overview.getBucketDivision().getValue0()).append(",")
                .append(overview.getBucketDivision().getValue1()).append(",")
                .append(overview.getBucketDivision().getValue2()).append(",");
        
        
        builder.append("\n");

        return builder.toString();
    }

    
}
