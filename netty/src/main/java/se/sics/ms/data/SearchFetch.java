package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.Collection;
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
        private Collection<ApplicationEntry.ApplicationEntryId> entryIds;


        public Request(UUID fetchRequestId, Collection<ApplicationEntry.ApplicationEntryId> entryIds) {

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

        public Collection<ApplicationEntry.ApplicationEntryId> getEntryIds() {
            return entryIds;
        }
    }




    public static class Response {


        private UUID fetchRequestId;
        private Collection<ApplicationEntry> applicationEntries;

        public Response(UUID fetchRequestId, Collection<ApplicationEntry> applicationEntries){

            this.fetchRequestId = fetchRequestId;
            this.applicationEntries = applicationEntries;

        }

        @Override
        public String toString() {
            return "Response{" +
                    "fetchRequestId=" + fetchRequestId +
                    ", applicationEntries=" + applicationEntries +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (applicationEntries != null ? !applicationEntries.equals(response.applicationEntries) : response.applicationEntries != null)
                return false;
            if (fetchRequestId != null ? !fetchRequestId.equals(response.fetchRequestId) : response.fetchRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = fetchRequestId != null ? fetchRequestId.hashCode() : 0;
            result = 31 * result + (applicationEntries != null ? applicationEntries.hashCode() : 0);
            return result;
        }

        public UUID getFetchRequestId() {
            return fetchRequestId;
        }

        public Collection<ApplicationEntry> getApplicationEntries() {
            return applicationEntries;
        }
    }




}
