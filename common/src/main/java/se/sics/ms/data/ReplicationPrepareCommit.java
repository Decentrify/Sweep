package se.sics.ms.data;

import se.sics.ms.types.IndexEntry;

import java.util.UUID;

/**
 * Container for the information passed as part of the request / response
 * protocol of the index entry addition phase.
 *
 * Created by babbar on 2015-04-20.
 */
public class ReplicationPrepareCommit {

    public static class Request {

        private final IndexEntry entry;
        private final UUID indexAdditionRoundId;

        public Request(IndexEntry entry, UUID indexAdditionRoundId) {

            this.entry = entry;
            this.indexAdditionRoundId = indexAdditionRoundId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (entry != null ? !entry.equals(request.entry) : request.entry != null) return false;
            if (indexAdditionRoundId != null ? !indexAdditionRoundId.equals(request.indexAdditionRoundId) : request.indexAdditionRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = entry != null ? entry.hashCode() : 0;
            result = 31 * result + (indexAdditionRoundId != null ? indexAdditionRoundId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "entry=" + entry +
                    ", indexAdditionRoundId=" + indexAdditionRoundId +
                    '}';
        }

        public IndexEntry getEntry() {
            return entry;
        }

        public UUID getIndexAdditionRoundId() {
            return indexAdditionRoundId;
        }
    }

    public static class Response {

        private final long entryId;
        private final UUID indexAdditionRoundId;

        public Response(UUID indexAdditionRoundId, long entryId){
            this.entryId = entryId;
            this.indexAdditionRoundId = indexAdditionRoundId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (entryId != response.entryId) return false;
            if (indexAdditionRoundId != null ? !indexAdditionRoundId.equals(response.indexAdditionRoundId) : response.indexAdditionRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (entryId ^ (entryId >>> 32));
            result = 31 * result + (indexAdditionRoundId != null ? indexAdditionRoundId.hashCode() : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "entryId=" + entryId +
                    ", indexAdditionRoundId=" + indexAdditionRoundId +
                    '}';
        }

        public long getEntryId() {
            return entryId;
        }

        public UUID getIndexAdditionRoundId() {
            return indexAdditionRoundId;
        }
    }

}
