package se.sics.ms.data;

import se.sics.ms.types.IndexEntry;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Once the nodes have added the index entries, the node actively tries to repair any gaps
 * in the entry space.
 *
 * Created by babbar on 2015-04-20.
 */
public class Repair {


    public static class Request {

        private final UUID repairRoundId;
        private final Long[] missingIds;

        public Request( UUID repairRoundId, Long[] missingIds){
            this.repairRoundId = repairRoundId;
            this.missingIds =missingIds;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (!Arrays.equals(missingIds, request.missingIds)) return false;
            if (repairRoundId != null ? !repairRoundId.equals(request.repairRoundId) : request.repairRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = repairRoundId != null ? repairRoundId.hashCode() : 0;
            result = 31 * result + (missingIds != null ? Arrays.hashCode(missingIds) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "repairRoundId=" + repairRoundId +
                    ", missingIds=" + Arrays.toString(missingIds) +
                    '}';
        }

        public UUID getRepairRoundId() {
            return repairRoundId;
        }

        public Long[] getMissingIds() {
            return missingIds;
        }
    }


    public static class Response{

        private final UUID repairRoundId;
        private final Collection<IndexEntry> missingEntries;

        public Response(UUID repairRoundId, Collection<IndexEntry> missingEntries){
            this.repairRoundId = repairRoundId;
            this.missingEntries = missingEntries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (missingEntries != null ? !missingEntries.equals(response.missingEntries) : response.missingEntries != null)
                return false;
            if (repairRoundId != null ? !repairRoundId.equals(response.repairRoundId) : response.repairRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = repairRoundId != null ? repairRoundId.hashCode() : 0;
            result = 31 * result + (missingEntries != null ? missingEntries.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "repairRoundId=" + repairRoundId +
                    ", missingEntries=" + missingEntries +
                    '}';
        }

        public UUID getRepairRoundId() {
            return repairRoundId;
        }

        public Collection<IndexEntry> getMissingEntries() {
            return missingEntries;
        }
    }

}
