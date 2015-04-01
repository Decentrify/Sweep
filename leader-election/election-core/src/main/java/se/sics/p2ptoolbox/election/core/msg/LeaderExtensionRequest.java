package se.sics.p2ptoolbox.election.core.msg;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.p2ptoolbox.election.core.data.ExtensionRequest;
import se.sics.p2ptoolbox.election.core.data.LeaseCommit;
import se.sics.p2ptoolbox.serialization.msg.NetContentMsg;

import java.util.UUID;

/**
 * Leader is extending the lease for the nodes.
 *
 * Created by babbar on 2015-03-31.
 */
public class LeaderExtensionRequest extends NetContentMsg.OneWay<ExtensionRequest>{

    public LeaderExtensionRequest(VodAddress src, VodAddress dest, UUID id, ExtensionRequest content) {
        super(src, dest, id, content);
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderExtensionRequest(vodSrc, vodDest, id, content);
    }
}
