package se.sics.ms.common;

import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.vod.VodView;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.net.Nat;
import se.sics.gvod.net.VodAddress;

import java.net.InetAddress;

/**
 *
 * @author: Steffen Grohsschmiedt
 */
public class MsSelfImpl extends SelfImpl {
    private long numberOfIndexEntries = 0;

    public MsSelfImpl(VodAddress addr) {
        super(addr);
    }

    public MsSelfImpl(Nat nat, InetAddress ip, int port, int nodeId, int overlayId) {
        super(nat, ip, port, nodeId, overlayId);
    }

    @Override
    public VodDescriptor getDescriptor() {
        int age = 0;
        return  new VodDescriptor(getAddress(), VodView.getPeerUtility(this), age, VodConfig.LB_MTU_MEASURED, numberOfIndexEntries);
    }

    public void setNumberOfIndexEntries(long numberOfIndexEntries) {
        this.numberOfIndexEntries = numberOfIndexEntries;
    }

    public void incrementNumberOfIndexEntries() {
        numberOfIndexEntries++;
    }
}
