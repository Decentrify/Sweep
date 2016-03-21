package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.LeaderUnit;

import java.util.UUID;

/**
 * Special Landing Entry Container Object.
 * Created by babbarshaer on 2015-05-04.
 */
public class LandingEntryAddPrepare {
    
    public static class Request extends EntryAddPrepare.Request{

        private LeaderUnit previousEpochUpdate;
        
        public Request(UUID entryAdditionRound, ApplicationEntry entry, LeaderUnit previousEpochUpdate) {
            super(entryAdditionRound, entry);
            this.previousEpochUpdate = previousEpochUpdate;
        }

        @Override
        public boolean equals(Object o) {
            
            if(o == this) return true;
            if(! (o instanceof  Request)) return false;
            
            Request request = (Request)o;
            if (entry != null ? !entry.equals(request.entry) : request.entry != null) return false;
            if (entryAdditionRound != null ? !entryAdditionRound.equals(request.entryAdditionRound) : request.entryAdditionRound != null) return false;
            if(previousEpochUpdate != null ? !previousEpochUpdate.equals(request.previousEpochUpdate) : request.previousEpochUpdate != null) return false;
            
            return true;
        }


        @Override
        public int hashCode() {
            
            int result = super.hashCode();
            result += 31 * result + (previousEpochUpdate != null ? previousEpochUpdate.hashCode() : 0);
            return result;
        }

        public LeaderUnit getPreviousEpochUpdate() {
            return previousEpochUpdate;
        }
    }
    

    public static class Response extends EntryAddPrepare.Response{

        public Response(UUID entryAdditionRound, ApplicationEntry.ApplicationEntryId entryId) {
            super(entryAdditionRound, entryId);
        }

        @Override
        public boolean equals(Object o) {

            if(o == this) return true;
            if(! (o instanceof  Response)) return false;
            
            Response response = (Response)o;
            return this.entryAdditionRound.equals(response.entryAdditionRound);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
    
}
