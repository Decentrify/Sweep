package se.sics.ms.data;

import se.sics.ms.types.IndexEntry;

import java.util.UUID;

/**
 * Index Entry Addition Request / Response Container.
 *
 * Created by babbarshaer on 2015-04-18.
 */
public class AddIndexEntry {
    
    public static class Request {
        
        private final UUID entryAdditionRound;
        private final IndexEntry entry;
        
        public Request(UUID entryAdditionRound, IndexEntry entry){
            this.entryAdditionRound = entryAdditionRound;
            this.entry = entry;
        }
        
        public UUID getEntryAdditionRound(){
            return this.entryAdditionRound;
        }
        
        public IndexEntry getEntry(){
            return this.entry;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

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
    
    public static class Response {
        
        private final UUID electionRoundId;
        
        public Response(UUID electionRoundId){
            this.electionRoundId = electionRoundId;
        }
        
        public UUID getElectionRoundId(){
            return this.electionRoundId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (electionRoundId != null ? !electionRoundId.equals(response.electionRoundId) : response.electionRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return electionRoundId != null ? electionRoundId.hashCode() : 0;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "electionRoundId=" + electionRoundId +
                    '}';
        }
    }
}
