package se.sics.ms.data;

import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.IdScorePair;

import java.util.Collection;
import java.util.UUID;

/**
 * Represents the query phase of the search request.
 * As part of this protocol, the nodes simply exchange
 * meta information about the responses that match the search request.
 *
 * Created by babbar on 2015-07-15.
 */
public class SearchQuery {


    public static class Request {

        private final UUID requestId;
        private final int partitionId;
        private final SearchPattern pattern;


        public Request(UUID requestId, int partitionId, SearchPattern pattern) {

            this.requestId = requestId;
            this.partitionId = partitionId;
            this.pattern = pattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (partitionId != request.partitionId) return false;
            if (pattern != null ? !pattern.equals(request.pattern) : request.pattern != null) return false;
            if (requestId != null ? !requestId.equals(request.requestId) : request.requestId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = requestId != null ? requestId.hashCode() : 0;
            result = 31 * result + partitionId;
            result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "requestId=" + requestId +
                    ", partitionId=" + partitionId +
                    ", pattern=" + pattern +
                    '}';
        }

        public UUID getRequestId() {
            return requestId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public SearchPattern getPattern() {
            return pattern;
        }
    }


    /**
     * The Response part of the query phase during the search request.
     * It mainly contains the information regarding the ids for the document that matched
     *
     */
    public static class Response {

        private final UUID searchTimeoutId;
        private final int partitionId;
        private final Collection<IdScorePair> idScorePairCollection;


        public Response(UUID searchTimeoutId, int partitionId, Collection<IdScorePair> idScorePairCollection) {

            this.searchTimeoutId = searchTimeoutId;
            this.partitionId = partitionId;
            this.idScorePairCollection = idScorePairCollection;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (partitionId != response.partitionId) return false;
            if (idScorePairCollection != null ? !idScorePairCollection.equals(response.idScorePairCollection) : response.idScorePairCollection != null)
                return false;
            if (searchTimeoutId != null ? !searchTimeoutId.equals(response.searchTimeoutId) : response.searchTimeoutId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = searchTimeoutId != null ? searchTimeoutId.hashCode() : 0;
            result = 31 * result + partitionId;
            result = 31 * result + (idScorePairCollection != null ? idScorePairCollection.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "searchTimeoutId=" + searchTimeoutId +
                    ", partitionId=" + partitionId +
                    ", idScorePairCollection=" + idScorePairCollection +
                    '}';
        }

        public UUID getSearchTimeoutId() {
            return searchTimeoutId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Collection<IdScorePair> getIdScorePairCollection() {
            return idScorePairCollection;
        }
    }






}
