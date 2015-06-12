package se.sics.ms.types;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;

/**
 * Object wrapper representing the marker entry in the system.
 *
 * Created by babbarshaer on 2015-06-12.
 */
public class MarkerEntry {
    
    private static final String EPOCH_ID = "markerEpoch";
    private static final String LEADER_ID = "markerLeader";
    
    private long epochId;
    private int leaderId;
    
    public MarkerEntry(long epochId, int leaderId){
        
        this.epochId = epochId;
        this.leaderId = leaderId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MarkerEntry that = (MarkerEntry) o;

        if (epochId != that.epochId) return false;
        if (leaderId != that.leaderId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (epochId ^ (epochId >>> 32));
        result = 31 * result + leaderId;
        return result;
    }

    public long getEpochId() {
        return epochId;
    }

    public int getLeaderId() {
        return leaderId;
    }
    
    
    public static class MarkerEntryHelper{
        

        /**
         * Create Marker Entry from the document in the system.
         *
         * @param doc document
         * @return Marker Entry.
         */
        public static MarkerEntry createEntryFromDocument(Document doc){

            long epochId = Long.valueOf(doc.get(MarkerEntry.EPOCH_ID));
            int leaderId = Integer.valueOf(doc.get(MarkerEntry.LEADER_ID));
            
            return new MarkerEntry(epochId, leaderId);
        }
        
        
        
        /**
         * Create a document from the market entry.
         *
         * @param markerEntry marker entry.
         * @return Document.
         */
        public static Document createDocumentFromEntry(MarkerEntry markerEntry){
            
            Document doc = new Document();
            doc.add(new LongField(MarkerEntry.EPOCH_ID, markerEntry.getEpochId(), Field.Store.YES));
            doc.add(new IntField(MarkerEntry.LEADER_ID, markerEntry.getLeaderId(), Field.Store.YES));
            
            return doc;
        }

    }
    
}
