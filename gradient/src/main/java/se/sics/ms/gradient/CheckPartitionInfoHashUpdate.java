package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;

/**
 * @author babbarshaer
 */
public class CheckPartitionInfoHashUpdate {

    /**
     * Check if any partitioning updates need to be sent to the node lagging behind.
     */
    public static class Request extends Event {

        private VodAddress sourceAddress;
        private TimeoutId roundId;

        public Request(TimeoutId roundId , VodAddress sourceAddress){
            this.sourceAddress = sourceAddress;
            this.roundId = roundId;
        }

        public VodAddress getSourceAddress(){
            return this.sourceAddress;
        }

        public TimeoutId getRoundId(){
            return this.roundId;
        }

    }


    /**
     * Contains Information Regarding the missing Partitioning Updates.
     */
    public static class Response extends Event{

        private TimeoutId roundId;
        private VodAddress sourceAddress;
        private ControlMessageEnum controlMessageEnum;
        private LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;

        public Response(TimeoutId roundId, VodAddress sourceAddress, LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes, ControlMessageEnum controlMessageEnum){
            this.roundId = roundId;
            this.sourceAddress = sourceAddress;
            this.partitionUpdateHashes = partitionUpdateHashes;
            this.controlMessageEnum = controlMessageEnum;
        }

        public TimeoutId getRoundId(){
            return this.roundId;
        }

        public VodAddress getSourceAddress(){
            return this.sourceAddress;
        }

        public ControlMessageEnum getControlMessageEnum(){
            return this.controlMessageEnum;
        }


        public LinkedList<PartitionHelper.PartitionInfoHash> getPartitionUpdateHashes(){
            return this.partitionUpdateHashes;
        }
    }

}
