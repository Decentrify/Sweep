package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.Collection;
import java.util.UUID;

/**
 * Main Class for the actual entry exchange in the system.
 * Once the hashes have been matched by the user, the user requests for the actual entries from the users.
 *
 * Created by babbar on 2015-05-13.
 */
public class EntryExchange {

    public static class Request {

        private final UUID exchangeRoundId;
        private final Collection<ApplicationEntry.ApplicationEntryId> entryIds;

        public Request(UUID exchangeRoundId, Collection<ApplicationEntry.ApplicationEntryId> entryIds){
            this.exchangeRoundId = exchangeRoundId;
            this.entryIds= entryIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (entryIds != null ? !entryIds.equals(request.entryIds) : request.entryIds != null) return false;
            if (exchangeRoundId != null ? !exchangeRoundId.equals(request.exchangeRoundId) : request.exchangeRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = exchangeRoundId != null ? exchangeRoundId.hashCode() : 0;
            result = 31 * result + (entryIds != null ? entryIds.hashCode() : 0);
            return result;
        }

        public UUID getExchangeRoundId() {
            return exchangeRoundId;
        }

        public Collection<ApplicationEntry.ApplicationEntryId> getEntryIds() {
            return entryIds;
        }
    }

    public static class Response {

        private UUID entryExchangeRound;
        private Collection<ApplicationEntry> applicationEntries;
        
        public Response ( UUID entryExchangeRound, Collection<ApplicationEntry> applicationEntries){
            
            this.entryExchangeRound = entryExchangeRound;
            this.applicationEntries = applicationEntries;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (applicationEntries != null ? !applicationEntries.equals(response.applicationEntries) : response.applicationEntries != null)
                return false;
            if (entryExchangeRound != null ? !entryExchangeRound.equals(response.entryExchangeRound) : response.entryExchangeRound != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = entryExchangeRound != null ? entryExchangeRound.hashCode() : 0;
            result = 31 * result + (applicationEntries != null ? applicationEntries.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "entryExchangeRound=" + entryExchangeRound +
                    ", applicationEntries=" + applicationEntries +
                    '}';
        }

        public UUID getEntryExchangeRound() {
            return entryExchangeRound;
        }

        public Collection<ApplicationEntry> getApplicationEntries() {
            return applicationEntries;
        }
    }

}
