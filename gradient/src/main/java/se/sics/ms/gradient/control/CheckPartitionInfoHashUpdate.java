package se.sics.ms.gradient.control;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;

/**
 * @author babbarshaer
 */
public class CheckPartitionInfoHashUpdate {

    /**
     * Check if any partitioning updates need to be sent to the node lagging behind.
     */
    public static class Request extends ControlMessageInternal.Request {

        public Request(TimeoutId roundId , VodAddress sourceAddress){
            super(roundId, sourceAddress);
        }
    }


    /**
     * Contains Information Regarding the missing Partitioning Updates.
     */
    public static class Response extends ControlMessageInternal.Response {

        private LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;

        public Response(TimeoutId roundId, VodAddress sourceAddress, LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes, ControlMessageEnum controlMessageEnum){
            super(roundId, sourceAddress, controlMessageEnum);
            this.partitionUpdateHashes = partitionUpdateHashes;
        }

        public LinkedList<PartitionHelper.PartitionInfoHash> getPartitionUpdateHashes(){
            return this.partitionUpdateHashes;
        }
    }

}
