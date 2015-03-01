package se.sics.ms.events.simEvents;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.types.IndexEntry;

/**
 * Created by babbarshaer on 2015-03-01.
 */
public class AddIndexEntryP2pSimulated {
    
    
    public static class Request extends DirectMsgNetty.Request{

        private final IndexEntry entry;

        public Request(VodAddress source, VodAddress destination, IndexEntry entry) {
            super(source, destination);
            this.entry = entry;
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


        public IndexEntry getIndexEntry(){
            return this.entry;
        }
    }
}
