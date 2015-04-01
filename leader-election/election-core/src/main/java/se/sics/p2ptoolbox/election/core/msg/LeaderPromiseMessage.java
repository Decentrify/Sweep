package se.sics.p2ptoolbox.election.core.msg;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.serialization.msg.NetContentMsg;

import java.util.UUID;

/**
 * Promise Message which is sent by the leader 
 * to the followers asking for them if they see them fit as a leader.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class LeaderPromiseMessage {
    
    public static class Request extends NetContentMsg.Request<Promise.Request>{

        public Request(VodAddress src, VodAddress dest, UUID id, Promise.Request content) {
            super(src, dest, id, content);
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, id, content);
        }
    }
    
    public static class Response extends NetContentMsg.Response<Promise.Response>{

        public Response(VodAddress src, VodAddress dest, UUID id, Promise.Response content) {
            super(src, dest, id, content);
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, id, content);
        }
    }
    
    
    
    
}
