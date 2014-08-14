package se.sics.ms.model;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.util.PartitionHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babbarshaer
 *
 * Class to keep track of number of responses.
 * Created by babbarshaer on 2014-07-22.
 */
public class PartitionReplicationCount {

    // FIXME: Can result in Heap Size Issues. Better the reset the object instead of creating a new one.

    private final int leaderGroupSize;
//    private int responsesReceived =0;

    private List<VodAddress> leaderGroupNodesAddress;
    private final PartitionHelper.PartitionInfo partitionInfo;

    public PartitionReplicationCount(int leaderGroupSize, PartitionHelper.PartitionInfo partitionInfo){
        this.leaderGroupSize = leaderGroupSize;
        leaderGroupNodesAddress = new ArrayList<>();
        this.partitionInfo = partitionInfo;
    }


    public boolean incrementAndCheckResponse(VodAddress address){

        leaderGroupNodesAddress.add(address);
        if(leaderGroupNodesAddress.size()>= leaderGroupSize){
            return true;
        }

        return false;
    }


    /**
     * Clear the contents of presently held addresses.
     */
    public void resetLeaderGroupNodesAddress(){
        leaderGroupNodesAddress.clear();
    }


    public List<VodAddress> getLeaderGroupNodesAddress(){
        return this.leaderGroupNodesAddress;
    }

    public PartitionHelper.PartitionInfo getPartitionInfo(){
        return this.partitionInfo;
    }

}
