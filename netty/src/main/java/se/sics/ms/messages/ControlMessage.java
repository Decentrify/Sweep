package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.Transport;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.ms.net.MessageFrameDecoder;

/**
 * Control Message which will contain the control information.
 * @author babbarshaer
 */
public class ControlMessage {


   public static class Request extends DirectMsgNetty.Request{


       public Request(VodAddress source, VodAddress destination, TimeoutId roundId) {
           super(source, destination,Transport.UDT,roundId);
       }

       @Override
       public int getSize() {
           return getHeaderSize();
       }

       @Override
       public RewriteableMsg copy() {
           return new Request(vodSrc,vodDest,timeoutId);
       }

       /**
        * Encode the Request Object in form of byte array to be sent over the network.
        * @return
        * @throws MessageEncodingException
        */
       @Override
       public ByteBuf toByteArray() throws MessageEncodingException {
           ByteBuf buffer = createChannelBufferWithHeader();
           return buffer;
       }

       @Override
       public byte getOpcode() {
           return MessageFrameDecoder.CONTROL_MESSAGE_REQUEST;
       }

       /**
        *
        * @return roundId.
        */
       public TimeoutId getRoundId(){
           return this.timeoutId;
       }
   }


    /**
     * TODO: incorporate the enums in a generic way in this.
     * Control Message Response containing the Different Control Messages.
     */
    public static class Response extends DirectMsgNetty.Response{

        //FIXME: Add support for the enums.

        private byte[] bytes;

        public Response(VodAddress source, VodAddress destination,TimeoutId roundId , byte[] bytes) {
            super(source, destination,roundId);
//            this.roundId = roundId;
            this.bytes = bytes;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc,vodDest,timeoutId,bytes);
        }

        /**
         * Encode the Response Object to be sent over the network.
         * @return ByteBuffer.
         * @throws MessageEncodingException
         */
        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {

            ByteBuf buffer = createChannelBufferWithHeader();
//            UserTypesEncoderFactory.writeTimeoutId(buffer,roundId);
            UserTypesEncoderFactory.writeArrayBytes(buffer, bytes);

            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.CONTROL_MESSAGE_RESPONSE;
        }

        /**
         *
         * @return currentRoundId
         */
        public TimeoutId getRoundId(){
            return this.timeoutId;
        }

        /**
         *
         * @return byte array.
         */
        public byte[] getBytes(){
            return this.bytes;
        }

    }





}
