package se.sics.ms.gradient.control;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.LinkedList;
import java.util.UUID;

/**
 * @author babbarshaer
 */
public class CheckPartitionInfoHashUpdate {

    /**
     * Check if any partitioning updates need to be sent to the node lagging behind.
     */
    public static class Request extends ControlMessageInternal.Request {

        private OverlayId overlayId;

        public Request(UUID roundId , DecoratedAddress sourceAddress, OverlayId overlayId){
            super(roundId, sourceAddress);
            this.overlayId = overlayId;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }
    }


    /**
     * Contains Information Regarding the missing Partitioning Updates.
     */
    public static class Response extends ControlMessageInternal.Response {

        private LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;

        public Response(UUID roundId, DecoratedAddress sourceAddress, LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes, ControlMessageEnum controlMessageEnum){
            
            super(roundId, sourceAddress, controlMessageEnum);
            this.partitionUpdateHashes = partitionUpdateHashes;
        }

        public LinkedList<PartitionHelper.PartitionInfoHash> getPartitionUpdateHashes(){
            return this.partitionUpdateHashes;
        }
    }

}
