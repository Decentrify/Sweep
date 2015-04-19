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
        
    }
    
    public static class Response {
        
        private final UUID electionRoundId;
        
        public Response(UUID electionRoundId){
            this.electionRoundId = electionRoundId;
        }
        
        public UUID getElectionRoundId(){
            return this.electionRoundId;
        }
    }
}
