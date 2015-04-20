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

        public long getEntryId() {
            return entryId;
        }

        public UUID getIndexAdditionRoundId() {
            return indexAdditionRoundId;
        }
    }

}
