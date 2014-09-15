package se.sics.ms.common;

import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.vod.VodView;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.net.Nat;
import se.sics.gvod.net.VodAddress;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author: Steffen Grohsschmiedt
 */
public class MsSelfImpl extends SelfImpl {
    private static AtomicLong numberOfIndexEntries = new AtomicLong();

    public MsSelfImpl(VodAddress addr) {
        super(addr);
    }

    public MsSelfImpl(Nat nat, InetAddress ip, int port, int nodeId, int overlayId) {
        super(nat, ip, port, nodeId, overlayId);
    }

    @Override
    public VodDescriptor getDescriptor() {
        int age = 0;
        return  new VodDescriptor(getAddress(), VodView.getPeerUtility(this), age, VodConfig.LB_MTU_MEASURED,
                MsSelfImpl.numberOfIndexEntries.get());
    }

    public void setNumberOfIndexEntries(long numberOfIndexEntries) {
        MsSelfImpl.numberOfIndexEntries.set(numberOfIndexEntries); 
    }

    public void incrementNumberOfIndexEntries() {
        MsSelfImpl.numberOfIndexEntries.incrementAndGet();
    }

    public void setOverlayId(int overlayId) {
        this.overlayId = overlayId;
    }

    public int getCategoryId() {
        return overlayId & 65535;
    }

    public int getPartitionId() {
        return (overlayId & 67043328) >>> 16;
    }

    public int getPartitionIdDepth() {
        return (overlayId & 1006632960) >>> 26;
    }

    public VodAddress.PartitioningType getPartitioningType() {
        return VodAddress.PartitioningType.values()[(overlayId & -1073741824) >>> 30];
    }

}
