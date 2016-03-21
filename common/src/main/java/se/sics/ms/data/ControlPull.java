package se.sics.ms.data;

import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.OverlayId;

import java.security.PublicKey;
import java.util.List;
import java.util.UUID;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Main class for the Control pull Mechanism in the system.
 *
 * Created by babbar on 2015-05-15.
 */
public class ControlPull {
    public static class Request {

        private UUID pullRound;
        private OverlayId overlayId;
        private LeaderUnit leaderUnit;


        public Request(UUID pullRound, OverlayId overlayId, LeaderUnit leaderUnit) {
            this.pullRound = pullRound;
            this.overlayId = overlayId;
            this.leaderUnit = leaderUnit;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (leaderUnit != null ? !leaderUnit.equals(request.leaderUnit) : request.leaderUnit != null)
                return false;
            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (pullRound != null ? !pullRound.equals(request.pullRound) : request.pullRound != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = pullRound != null ? pullRound.hashCode() : 0;
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            result = 31 * result + (leaderUnit != null ? leaderUnit.hashCode() : 0);
            return result;
        }

        public UUID getPullRound() {
            return pullRound;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }

        public LeaderUnit getLeaderUnit() {
            return leaderUnit;
        }
    }


    /**
     * As all the information is present in one component,
     * not creating a separate generic pull mechanism for now.
     *
     * Will be refactoring it once we have a stable version.
     *
     */
    public static class Response {


        private UUID pullRound;
        private KAddress leaderAddress;
        private PublicKey leaderKey;
        private List<LeaderUnit> nextUpdates;
        private final int overlayId;


        public Response(UUID pullRound, KAddress leaderAddress,PublicKey leaderKey,  List<LeaderUnit> nextUpdates, int overlayId) {
            this.pullRound = pullRound;
            this.leaderAddress = leaderAddress;
            this.leaderKey = leaderKey;
            this.nextUpdates = nextUpdates;
            this.overlayId = overlayId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (overlayId != response.overlayId) return false;
            if (leaderAddress != null ? !leaderAddress.getId().equals(response.leaderAddress.getId()) : response.leaderAddress != null)
                return false;
            if (leaderKey != null ? !leaderKey.equals(response.leaderKey) : response.leaderKey != null) return false;
            if (nextUpdates != null ? !nextUpdates.equals(response.nextUpdates) : response.nextUpdates != null)
                return false;
            if (pullRound != null ? !pullRound.equals(response.pullRound) : response.pullRound != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = pullRound != null ? pullRound.hashCode() : 0;
            result = 31 * result + (leaderAddress != null ? leaderAddress.getId().hashCode() : 0);
            result = 31 * result + (leaderKey != null ? leaderKey.hashCode() : 0);
            result = 31 * result + (nextUpdates != null ? nextUpdates.hashCode() : 0);
            result = 31 * result + overlayId;
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "pullRound=" + pullRound +
                    ", leaderAddress=" + leaderAddress +
                    ", leaderKey=" + leaderKey +
                    ", nextUpdates=" + nextUpdates +
                    ", overlayId=" + overlayId +
                    '}';
        }

        public int getOverlayId() {
            return overlayId;
        }

        public PublicKey getLeaderKey() {
            return leaderKey;
        }

        public UUID getPullRound() {
            return pullRound;
        }

        public KAddress getLeaderAddress() {
            return leaderAddress;
        }

        public List<LeaderUnit> getNextUpdates() {
            return nextUpdates;
        }
    }




}
