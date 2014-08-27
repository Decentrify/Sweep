package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.util.PartitionHelper;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Message used to inform the nodes about the partition update history.
 * @author babbarshaer.
 */
public class DelayedPartitioningMessage {

    //TODO: Write the additional test cases in the encoding decoding test.
    /**
     * Request sent as part Delayed Partitioning Update.
     */
    public static class Request extends DirectMsgNetty.Request{


        private List<TimeoutId> partitionRequestIds = new ArrayList<TimeoutId>();

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId , List<TimeoutId> partitionRequestIds) {
            super(source, destination, timeoutId);
            this.partitionRequestIds = partitionRequestIds;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc,vodDest,timeoutId,partitionRequestIds);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {

            ByteBuf byteBuf = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writePartitionRequestIds(byteBuf, partitionRequestIds);
            return byteBuf;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.DELAYED_PARTITIONING_MESSAGE_REQUEST;
        }

        public List<TimeoutId> getPartitionRequestIds(){
            return this.partitionRequestIds;
        }

    }


    /**
     * Response sent as part of Partitioning Update.
     */
    public static class Response extends DirectMsgNetty.Response{

        LinkedList<PartitionHelper.PartitionInfo> partitionHistory;

        public Response(VodAddress source, VodAddress destination , TimeoutId timeoutId, LinkedList<PartitionHelper.PartitionInfo> partitionHistory) {
            super(source, destination, timeoutId);
            this.partitionHistory = partitionHistory;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc,vodDest,timeoutId, partitionHistory);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeDelayedPartitionInfo(buffer,partitionHistory);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.DELAYED_PARTITIONING_MESSAGE_RESPONSE;
        }

        public LinkedList<PartitionHelper.PartitionInfo> getPartitionHistory(){
            return this.partitionHistory;
        }
    }


    /**
     * Timeout as part of Delayed Partitioning Update.
     */
    public static class Timeout extends IndividualTimeout{

        public Timeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }





}
