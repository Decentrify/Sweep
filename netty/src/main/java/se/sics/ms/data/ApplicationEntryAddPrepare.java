package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.UUID;

/**
 * Index Entry Addition Request / Response Container.
 *
 * Created by babbarshaer on 2015-04-18.
 */
public class ApplicationEntryAddPrepare {
    
    public static class Request extends EntryAddPrepare.Request{

        public Request(UUID entryAdditionRound, ApplicationEntry entry) {
            super(entryAdditionRound, entry);
        }


        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }


    public static class Response extends EntryAddPrepare.Response{

        public Response(UUID entryAdditionRound) {
            super(entryAdditionRound);
        }
    }

}
