package se.sics.ms.control;

import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.security.PublicKey;

/**
 * Leader Info Update received through the control pull mechanism.
 * Created by alidar on 8/12/14.
 */
public class LeaderInfoControlResponse extends ControlBase {

    private DecoratedAddress leaderAddress;
    private PublicKey leaderPublicKey;

    public LeaderInfoControlResponse(DecoratedAddress leaderAddress, PublicKey leaderPublicKey)
    {
        super(ControlMessageResponseTypeEnum.LEADER_UPDATE_RESPONSE);
        this.leaderAddress = leaderAddress;
        this.leaderPublicKey = leaderPublicKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderInfoControlResponse that = (LeaderInfoControlResponse) o;

        if (leaderAddress != null ? !leaderAddress.equals(that.leaderAddress) : that.leaderAddress != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return leaderAddress != null ? leaderAddress.hashCode() : 0;
    }

    public DecoratedAddress getLeaderAddress() {
        return leaderAddress;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }
}
