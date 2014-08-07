package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;
import java.util.List;

/**
 * @author babbarshaer
 */
public class CheckPartitionInfo {

    /**
     * Provides the list of required partition updates in the specified sequence.
     */
    public static class Request extends Event {

        private VodAddress sourceAddress;
        private TimeoutId roundId;
        private List<TimeoutId> partitionUpdateIds;

        public Request(TimeoutId roundId , VodAddress sourceAddress, List<TimeoutId> partitionUpdateIds){
            this.sourceAddress = sourceAddress;
            this.roundId = roundId;
            this.partitionUpdateIds = partitionUpdateIds;
        }

        public VodAddress getSourceAddress(){
            return this.sourceAddress;
        }

        public TimeoutId getRoundId(){
            return this.roundId;
        }

        public List<TimeoutId> getPartitionUpdateIds(){
            return this.partitionUpdateIds;
        }

    }


    /**
     * Contains Information Regarding the missing Partitioning Updates.
     */
    public static class Response extends Event{

        private TimeoutId roundId;
        private VodAddress sourceAddress;
        private LinkedList<PartitionHelper.PartitionInfo> partitionUpdates;

        public Response(TimeoutId roundId, VodAddress sourceAddress, LinkedList<PartitionHelper.PartitionInfo> partitionUpdates){
            this.roundId = roundId;
            this.sourceAddress = sourceAddress;
            this.partitionUpdates = partitionUpdates;
        }

        public TimeoutId getRoundId(){
            return this.roundId;
        }

        public VodAddress getSourceAddress(){
            return this.sourceAddress;
        }

        public LinkedList<PartitionHelper.PartitionInfo> getPartitionUpdates(){
            return this.partitionUpdates;
        }


    }

}
