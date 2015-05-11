package se.sics.ms.simulation;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.util.PartitionHelper;

import java.util.*;

/**
 *
 * Created by babbarshaer on 2015-05-08.
 */
public class Test {

    private static Map<Integer, Set<Integer>> nodeList = new HashMap<Integer, Set<Integer>>();

    public static void main(String[] args) {
        
//        int seed = 10;
//        Random random = new Random(seed);
//        generateNodeList(3, 2, random);
//        
//        System.out.println(" Final Map: " + nodeList);
        int nodeId = -1949530811;
        PartitionId partitionId = new PartitionId(VodAddress.PartitioningType.ONCE_BEFORE, 1, 1);
        boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(nodeId, partitionId);
        System.out.println(partitionSubId);

        int newPartitionId = partitionId.getPartitionId() | ((partitionSubId ? 1 : 0) << partitionId.getPartitionId());
        System.out.println(newPartitionId);
        
    }
    
    
    
    private static void generateNodeList(int depth, int bucketSize, Random random){
        
        List<Boolean> filledBuckets = new ArrayList<Boolean>();
        
        while(filledBuckets.size() < Math.pow(2, depth)){
            
            int id = random.nextInt();
            int genBucketId = generateBucketId(id, depth);
            Set<Integer> idSet = nodeList.get(genBucketId);
            
            if(idSet == null){
                idSet = new HashSet<Integer>();
                nodeList.put(genBucketId, idSet);
            }
            
            else if(idSet.size() == bucketSize){
                continue;
            }
            
            idSet.add(id);
            if(idSet.size() == bucketSize){
                filledBuckets.add(true);
            }
        }

    }
    
    
    
    private static int generateBucketId(int nodeId, int depth){
           
        int partition =0;
        for(int i=0; i< depth; i++){
           partition = partition | (nodeId & (1 << i));
        }
        
        return partition;
    }
    
}
