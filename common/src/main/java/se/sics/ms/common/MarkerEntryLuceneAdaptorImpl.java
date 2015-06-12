package se.sics.ms.common;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import se.sics.ms.types.MarkerEntry;

/**
 * Main implementation class for marker entries in the 
 * system.
 *
 * Created by babbarshaer on 2015-06-12.
 */
public class MarkerEntryLuceneAdaptorImpl extends MarkerEntryLuceneAdaptor {
    
    public MarkerEntryLuceneAdaptorImpl(Directory directory, IndexWriterConfig config) {
        super(directory, config);
    }

    @Override
    public MarkerEntry getLastEntry() {
        return null;
    }
}
