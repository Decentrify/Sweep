package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.PartitionHelper;

/**
 * Prepare Message to Inform Leader Group about the Partitioning Commencement.
 * @author babbarshaer
 */
public class PartitionPrepareMessage {


    public static class Request extends DirectMsgNetty.Request {

        private final PartitionHelper.PartitionInfo partitionInfo;
        private final OverlayId overlayId;


        public Request(VodAddress source, VodAddress destination, OverlayId overlayId, TimeoutId roundId, PartitionHelper.PartitionInfo partitionInfo){
            super(source, destination, roundId);
            this.partitionInfo = partitionInfo;
            this.overlayId = overlayId;
        }


        public PartitionHelper.PartitionInfo getPartitionInfo(){
            return this.partitionInfo;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }


        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, overlayId, timeoutId, partitionInfo);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writePartitionUpdate(buffer, partitionInfo);
            ApplicationTypesEncoderFactory.writeOverlayId(buffer, overlayId);
            return buffer;
        }

        //TODO: Create a new one.
        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.PARTITION_PREPARE_REQUEST;
        }
    }


    /**
     * Prepare Phase Response from the leader group.
     */
    public static class Response extends DirectMsgNetty.Response {

        private final TimeoutId partitionRequestId;

        public Response(VodAddress source, VodAddress destination, TimeoutId roundId ,TimeoutId partitionRequestId) {
            super(source, destination, roundId);
            this.partitionRequestId = partitionRequestId;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, partitionRequestId);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {

            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeTimeoutId(buffer,partitionRequestId);
            return buffer;
        }

        //TODO: Create a new one.
        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.PARTITION_PREPARE_RESPONSE;
        }

        public TimeoutId getPartitionRequestId(){
            return this.partitionRequestId;
        }

    }


    /**
     * Timeout Specific to the Prepare Request.
     */
    public static class Timeout extends IndividualTimeout{

        private final PartitionHelper.PartitionInfo partitionInfo;

        public Timeout(ScheduleTimeout request, int id , PartitionHelper.PartitionInfo partitionInfo) {
            super(request, id);
            this.partitionInfo = partitionInfo;
        }

        public PartitionHelper.PartitionInfo getPartitionInfo(){
            return this.partitionInfo;
        }

    }


}
