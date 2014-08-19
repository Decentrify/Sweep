package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;

/**
 * This is the factory class for the Control Message.
 * @author babbarshaer
 */
public class ControlMessageFactory {

    public static class Request extends DirectMsgNettyFactory.Request{

        private Request(){}

        public static ControlMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ControlMessage.Request) new ControlMessageFactory.Request().decode(buffer);
        }

        /**
         * Processing the message which involves decoding the message.
         * @param byteBuf
         * @return
         * @throws MessageDecodingException
         */
        @Override
        protected ControlMessage.Request process(ByteBuf byteBuf) throws MessageDecodingException {

            TimeoutId roundId = UserTypesDecoderFactory.readTimeoutId(byteBuf);
            return new ControlMessage.Request(vodSrc,vodDest,roundId);
        }
    }


    /**
     * Control Message Response containing updates.
     *
     */
    public static class Response extends DirectMsgNettyFactory.Response{

        private Response(){}

        public static ControlMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ControlMessage.Response) new ControlMessageFactory.Response().decode(buffer);
        }

        /**
         * Decoding the message to convert it to actual Response Object.
         * @param byteBuf
         * @return
         * @throws MessageDecodingException
         */
        @Override
        protected ControlMessage.Response process(ByteBuf byteBuf) throws MessageDecodingException {

            TimeoutId roundId = UserTypesDecoderFactory.readTimeoutId(byteBuf);
            byte[] bytes = UserTypesDecoderFactory.readArrayBytes(byteBuf);

            return new ControlMessage.Response(vodSrc, vodDest, roundId, bytes);
        }
    }



}
