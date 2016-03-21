package se.sics.ms.data;

import se.sics.ms.util.EntryScorePair;
import se.sics.ms.util.IdScorePair;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The fetch phase of the search request protocol.
 * In this the node simply GET's the actual entries based on the identifier sent
 * in the request.
 *
 * Created by babbar on 2015-07-15.
 */
public class SearchFetch {


    public static class Request {

        private UUID fetchRequestId;
        private List<IdScorePair> entryIds;


        public Request(UUID fetchRequestId, List<IdScorePair> entryIds) {

            this.fetchRequestId = fetchRequestId;
            this.entryIds = entryIds;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "fetchRequestId=" + fetchRequestId +
                    ", entryIds=" + entryIds +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (entryIds != null ? !entryIds.equals(request.entryIds) : request.entryIds != null) return false;
            if (fetchRequestId != null ? !fetchRequestId.equals(request.fetchRequestId) : request.fetchRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = fetchRequestId != null ? fetchRequestId.hashCode() : 0;
            result = 31 * result + (entryIds != null ? entryIds.hashCode() : 0);
            return result;
        }

        public UUID getFetchRequestId() {
            return fetchRequestId;
        }

        public List<IdScorePair> getEntryIds() {
            return entryIds;
        }
    }




    public static class Response {


        private UUID fetchRequestId;
        private List<EntryScorePair> entryScorePairs;

        public Response(UUID fetchRequestId, List<EntryScorePair> entryScorePairs){

            this.fetchRequestId = fetchRequestId;
            this.entryScorePairs = entryScorePairs;

        }

        @Override
        public String toString() {
            return "Response{" +
                    "fetchRequestId=" + fetchRequestId +
                    ", applicationEntries=" + entryScorePairs +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (entryScorePairs != null ? !entryScorePairs.equals(response.entryScorePairs) : response.entryScorePairs != null)
                return false;
            if (fetchRequestId != null ? !fetchRequestId.equals(response.fetchRequestId) : response.fetchRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = fetchRequestId != null ? fetchRequestId.hashCode() : 0;
            result = 31 * result + (entryScorePairs != null ? entryScorePairs.hashCode() : 0);
            return result;
        }

        public UUID getFetchRequestId() {
            return fetchRequestId;
        }

        public List<EntryScorePair> getEntryScorePairs() {
            return entryScorePairs;
        }
    }




}
