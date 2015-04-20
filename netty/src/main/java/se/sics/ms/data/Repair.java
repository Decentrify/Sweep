package se.sics.ms.data;

import se.sics.ms.types.IndexEntry;

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

        public UUID getRepairRoundId() {
            return repairRoundId;
        }

        public Collection<IndexEntry> getMissingEntries() {
            return missingEntries;
        }
    }

}
