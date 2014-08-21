package se.sics.ms.control;

import se.sics.gvod.net.VodAddress;

import java.security.PublicKey;

/**
 * Created by alidar on 8/12/14.
 */
public class LeaderInfoControlResponse extends ControlBase {

    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;

    public LeaderInfoControlResponse(VodAddress leaderAddress, PublicKey leaderPublicKey)
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

    public VodAddress getLeaderAddress() {
        return leaderAddress;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }
}
