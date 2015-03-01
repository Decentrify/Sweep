package se.sics.ms.events.simEvents;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;

/**
 * Created by babbarshaer on 2015-02-04.
 */
public class PeerJoinP2pSimulated {
    
    public static class Request extends DirectMsgNetty.Request{

        private final Long peerId;
        
        public Request(VodAddress source, VodAddress destination, Long peerId) {
            super(source, destination);
            this.peerId = peerId;
        }

        @Override
        public int getSize() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public RewriteableMsg copy() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public ByteBuf toByteArray() throws MessageEncodingException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public byte getOpcode() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        
        public Long getPeerId(){
            return this.peerId;
        }
    }
    
}
