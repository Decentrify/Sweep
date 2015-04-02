package se.sics.p2ptoolbox.election.example.msg;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;

import java.util.Collection;

/**
 * Message sent by the simulator to the application informing about the peers in the system.
 *
 * Created by babbar on 2015-04-01.
 */
public class AddPeers extends DirectMsgNetty.Request {

    public Collection<VodAddress> peers;

    public AddPeers(VodAddress source, VodAddress destination, Collection<VodAddress> peers) {
        super(source, destination);
        this.peers = peers;
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
