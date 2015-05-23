package se.sics.ms.data;

import se.sics.ms.types.EpochContainer;
import se.sics.ms.types.EpochUpdate;
import se.sics.ms.types.OverlayId;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.List;
import java.util.UUID;

/**
 * Main class for the Control pull Mechanism in the system.
 *
 * Created by babbar on 2015-05-15.
 */
public class ControlPull {



    public static class Request {

        private UUID pullRound;
        private OverlayId overlayId;
        private EpochContainer epochUpdate;


        public Request(UUID pullRound, OverlayId overlayId, EpochContainer epochUpdate) {
            this.pullRound = pullRound;
            this.overlayId = overlayId;
            this.epochUpdate = epochUpdate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (epochUpdate != null ? !epochUpdate.equals(request.epochUpdate) : request.epochUpdate != null)
                return false;
            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (pullRound != null ? !pullRound.equals(request.pullRound) : request.pullRound != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = pullRound != null ? pullRound.hashCode() : 0;
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            result = 31 * result + (epochUpdate != null ? epochUpdate.hashCode() : 0);
            return result;
        }

        public UUID getPullRound() {
            return pullRound;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }

        public EpochContainer getEpochUpdate() {
            return epochUpdate;
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
        private DecoratedAddress leaderAddress;
//        private EpochContainer modifiedUpdate;
        private List<EpochContainer> nextUpdates;


        public Response(UUID pullRound, DecoratedAddress leaderAddress, List<EpochContainer> nextUpdates) {
            this.pullRound = pullRound;
            this.leaderAddress = leaderAddress;
//            this.modifiedUpdate = modifiedUpdate;
            this.nextUpdates = nextUpdates;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (leaderAddress != null ? !leaderAddress.equals(response.leaderAddress) : response.leaderAddress != null)
                return false;
            if (nextUpdates != null ? !nextUpdates.equals(response.nextUpdates) : response.nextUpdates != null)
                return false;
            if (pullRound != null ? !pullRound.equals(response.pullRound) : response.pullRound != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = pullRound != null ? pullRound.hashCode() : 0;
            result = 31 * result + (leaderAddress != null ? leaderAddress.hashCode() : 0);
            result = 31 * result + (nextUpdates != null ? nextUpdates.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "pullRound=" + pullRound +
                    ", leaderAddress=" + leaderAddress +
                    ", nextUpdates=" + nextUpdates +
                    '}';
        }

        public UUID getPullRound() {
            return pullRound;
        }

        public DecoratedAddress getLeaderAddress() {
            return leaderAddress;
        }

        public List<EpochContainer> getNextUpdates() {
            return nextUpdates;
        }
    }




}
