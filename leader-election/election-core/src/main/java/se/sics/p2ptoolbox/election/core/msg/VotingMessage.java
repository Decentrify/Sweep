package se.sics.p2ptoolbox.election.core.msg;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.p2ptoolbox.election.core.data.VotingRequest;
import se.sics.p2ptoolbox.election.core.data.VotingResponse;
import se.sics.p2ptoolbox.serialization.msg.NetContentMsg;

import java.util.UUID;

/**
 * Wrapper Class for the voting protocol by the node asserting itself to be leader.
 *  
 * Created by babbarshaer on 2015-03-28.
 */
public class VotingMessage {

    public static class Request extends NetContentMsg.Request<VotingRequest>{

        public Request(VodAddress src, VodAddress dest, UUID id, VotingRequest content) {
            super(src, dest, id, content);
        }
        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, id, content);
        }
    }

    public static class Response extends NetContentMsg.Response<VotingResponse>{

        public Response(VodAddress src, VodAddress dest, UUID id, VotingResponse content) {
            super(src, dest, id, content);
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, id, content);
        }
    }
}
