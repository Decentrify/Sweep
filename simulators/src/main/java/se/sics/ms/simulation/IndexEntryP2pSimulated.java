package se.sics.ms.simulation;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;

/**
 * Created by babbarshaer on 2015-02-04.
 */
public class IndexEntryP2pSimulated {

    /**
     * Add Index Entry Request.
     */
    public static class Request extends DirectMsgNetty.Request{

        private final Long id;

        public Request(VodAddress source, VodAddress destination, Long id) {
            super(source, destination);
            this.id = id;
        }

        @Override
        public int getSize() {
            return 0;
        }

        @Override
        public RewriteableMsg copy() {
            return null;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            return null;
        }

        @Override
        public byte getOpcode() {
            return 0;
        }
        
        
        public Long getId(){
            return this.id;
        }
    }


    /**
     * Response for the IndexEntryAdd Request.
     */
    public static class Response extends DirectMsgNetty.Response{
        
        public Response(VodAddress source, VodAddress destination) {
            super(source, destination);
        }

        @Override
        public int getSize() {
            return 0;
        }

        @Override
        public RewriteableMsg copy() {
            return null;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            return null;
        }

        @Override
        public byte getOpcode() {
            return 0;
        }
    }
    
}
