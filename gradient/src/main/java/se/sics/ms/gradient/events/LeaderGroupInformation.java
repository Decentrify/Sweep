package se.sics.ms.gradient.events;


import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;

import java.util.List;

/**
 * Created by babbarshaer on 2014-07-23.
 */
public class LeaderGroupInformation {


    public static class Request extends Event{


        private final int leaderGroupSize;
        private final long medianId;
        private final VodAddress.PartitioningType partitioningType;

        public Request(long medianId, VodAddress.PartitioningType partitioningType, int leaderGroupSize){
            this.medianId = medianId;
            this.partitioningType = partitioningType;
            this.leaderGroupSize = leaderGroupSize;
        }

        public long getMedianId(){
            return medianId;
        }

        public VodAddress.PartitioningType getPartitioningType(){
            return this.partitioningType;
        }

        public int getLeaderGroupSize(){
            return this.leaderGroupSize;
        }

    }


    public static class Response extends Event {

        private final List<VodAddress> leaderGroupAddress;
        private final long medianId;
        private final VodAddress.PartitioningType partitioningType;

        public Response(long medianId, VodAddress.PartitioningType partitioningType, List<VodAddress> leaderGroupAddress){
            this.medianId = medianId;
            this.partitioningType = partitioningType;
            this.leaderGroupAddress = leaderGroupAddress;
        }

        public long getMedianId(){
            return medianId;
        }

        public VodAddress.PartitioningType getPartitioningType(){
            return this.partitioningType;
        }


        public List<VodAddress> getLeaderGroupAddress(){
            return this.leaderGroupAddress;
        }

    }

}
