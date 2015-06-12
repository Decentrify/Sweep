package se.sics.ms.common;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import se.sics.ms.types.MarkerEntry;

/**
 * Adaptor for adding marker entries in the system.
 *
 * Created by babbarshaer on 2015-06-12.
 */
public abstract class MarkerEntryLuceneAdaptor extends LuceneAdaptorBasic{

    public MarkerEntryLuceneAdaptor(Directory directory, IndexWriterConfig config) {
        super(directory, config);
    }


    /**
     * Sort the marker entries and check for the 
     * largest marker entry added in the system.
     * 
     * Priority in sorting is given to the epoch id and then to the leader unit id.
     * @return last marker entry
     */
    public abstract MarkerEntry getLastEntry();
}
