package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.UUID;

/**
 * Marker Interface for adding entry
 * Created by babbarshaer on 2015-05-04.
 */
public class EntryAddPrepare {

    public abstract static class Request {

        protected final UUID entryAdditionRound;
        protected final ApplicationEntry entry;

        public Request(UUID entryAdditionRound, ApplicationEntry entry){
            this.entryAdditionRound = entryAdditionRound;
            this.entry = entry;
        }

        public UUID getEntryAdditionRound(){
            return this.entryAdditionRound;
        }

        public ApplicationEntry getApplicationEntry(){
            return this.entry;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (! (o instanceof Request)) return false;

            Request request = (Request) o;

            if (entry != null ? !entry.equals(request.entry) : request.entry != null) return false;
            if (entryAdditionRound != null ? !entryAdditionRound.equals(request.entryAdditionRound) : request.entryAdditionRound != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = entryAdditionRound != null ? entryAdditionRound.hashCode() : 0;
            result = 31 * result + (entry != null ? entry.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "entryAdditionRound=" + entryAdditionRound +
                    ", entry=" + entry +
                    '}';
        }
    }

    public abstract static class Response {

        protected final UUID entryAdditionRound;
        protected final long entryId;

        public Response(UUID entryAdditionRound, long entryId){
            this.entryAdditionRound = entryAdditionRound;
            this.entryId = entryId;
        }

        public long getEntryId() {
            return entryId;
        }

        public UUID getEntryAdditionRound(){
            return this.entryAdditionRound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (! (o instanceof Response)) return false;

            Response response = (Response) o;

            if (entryAdditionRound != null ? !entryAdditionRound.equals(response.entryAdditionRound) : response.entryAdditionRound != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return entryAdditionRound != null ? entryAdditionRound.hashCode() : 0;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "electionRoundId=" + entryAdditionRound +
                    '}';
        }
    }

}
