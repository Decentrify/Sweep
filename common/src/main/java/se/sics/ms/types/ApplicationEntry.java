package se.sics.ms.types;

import org.apache.lucene.document.*;

/**
 * Composite Object keeping track of entries added in the system and other important metadata associated
 * with the entry added.
 *
 * @author babbar
 */
public class ApplicationEntry {

    public static String EPOCH_ID = "epochId";
    public static String LEADER_ID = "leaderId";
    public static String ENTRY_ID = "entryId";

    ApplicationEntryId applicationEntryId;
    private IndexEntry entry;


    public ApplicationEntry(ApplicationEntryId applicationEntryId){
        this.applicationEntryId = applicationEntryId;
        this.entry = IndexEntry.DEFAULT_ENTRY;
    }

    public ApplicationEntry(ApplicationEntryId applicationEntryId, IndexEntry entry){
        this.applicationEntryId=applicationEntryId;
        this.entry = entry;
    }


    @Override
    public String toString() {
        return "ApplicationEntry{" +
                "applicationEntryId=" + applicationEntryId +
                ", entry=" + entry +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ApplicationEntry that = (ApplicationEntry) o;

        if (applicationEntryId != null ? !applicationEntryId.equals(that.applicationEntryId) : that.applicationEntryId != null)
            return false;
        if (entry != null ? !entry.equals(that.entry) : that.entry != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = applicationEntryId != null ? applicationEntryId.hashCode() : 0;
        result = 31 * result + (entry != null ? entry.hashCode() : 0);
        return result;
    }

    public long getEpochId() {
        return this.applicationEntryId.getEpochId();
    }

    public int getLeaderId() {
        return this.applicationEntryId.getLeaderId();
    }

    public long getEntryId() {
        return this.applicationEntryId.getEntryId();
    }

    public IndexEntry getEntry() {
        return entry;
    }


    public static class ApplicationEntryId {
        
        private final long epochId;
        private final int leaderId;
        private final long entryId;
        
        public ApplicationEntryId(long epochId, int leaderId, long entryId){
            
            this.epochId = epochId;
            this.leaderId = leaderId;
            this.entryId = entryId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ApplicationEntryId that = (ApplicationEntryId) o;

            if (entryId != that.entryId) return false;
            if (epochId != that.epochId) return false;
            if (leaderId != that.leaderId) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (epochId ^ (epochId >>> 32));
            result = 31 * result + leaderId;
            result = 31 * result + (int) (entryId ^ (entryId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "ApplicationEntryId{" +
                    "epochId=" + epochId +
                    ", leaderId=" + leaderId +
                    ", entryId=" + entryId +
                    '}';
        }

        public long getEpochId() {
            return epochId;
        }

        public int getLeaderId() {
            return leaderId;
        }

        public long getEntryId() {
            return entryId;
        }
    }
    
    

    public static class ApplicationEntryHelper {


        /**
         * Given an instance of document, generate an instance of Entry.
         * @param d Document
         * @return Application Entry.
         */
        public static ApplicationEntry createApplicationEntryFromDocument(Document d){

            long epochId = Long.valueOf(d.get(ApplicationEntry.EPOCH_ID));
            int leaderId = Integer.valueOf(d.get(ApplicationEntry.LEADER_ID));
            long entryId = Long.valueOf(d.get(ApplicationEntry.ENTRY_ID));
            IndexEntry entry = IndexEntry.IndexEntryHelper.createIndexEntry(d);
            
            ApplicationEntryId applicationEntryId = new ApplicationEntryId(epochId, leaderId, entryId);
            return new ApplicationEntry(applicationEntryId, entry);
        }


        /**
         * Given an instance of Document, write the application entry to that document and return the
         * instance.
         *
         * @param doc Document
         * @param applicationEntry Application Entry.
         * @return Document
         */
        public static Document createDocumentFromEntry(Document doc, ApplicationEntry applicationEntry) {

            IndexEntry entry = applicationEntry.getEntry();

            // Storing Application Entry Data.
            doc.add(new LongField(ApplicationEntry.EPOCH_ID, applicationEntry.getEpochId(), Field.Store.YES));
            doc.add(new IntField(ApplicationEntry.LEADER_ID, applicationEntry.getLeaderId(), Field.Store.YES));
            doc.add(new LongField(ApplicationEntry.ENTRY_ID, applicationEntry.getEntryId(), Field.Store.YES));

            // Storing Index Entry Data.
            doc = IndexEntry.IndexEntryHelper.addIndexEntryToDocument(doc, entry);
            return doc;
        }
    }

}
