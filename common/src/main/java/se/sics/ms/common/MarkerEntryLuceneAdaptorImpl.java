package se.sics.ms.common;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.MarkerEntry;

import java.io.IOException;

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
    private static Logger logger = LoggerFactory.getLogger(MarkerEntryLuceneAdaptor.class);
    
    @Override
    public MarkerEntry getLastEntry() throws LuceneAdaptorException {
        
        Sort sort = new Sort(SortField.FIELD_SCORE,
                new SortField(MarkerEntry.EPOCH_ID, SortField.Type.LONG),
                new SortField(MarkerEntry.LEADER_ID, SortField.Type.INT));
        
        Query searchQuery = NumericRangeQuery.newLongRange(MarkerEntry.EPOCH_ID, 0l, Long.MAX_VALUE, true, true);
        
        try{

            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs docs = searcher.search(searchQuery, Integer.MAX_VALUE, sort);
            ScoreDoc[] hits = docs.scoreDocs;

            if(hits.length == 0){

                logger.warn("No Entries Found for the specified query.");
                return null;
            }
            
            int size = hits.length - 1; // Entry Entry.
            Document d = searcher.doc( hits[size].doc );
            return MarkerEntry.MarkerEntryHelper
                    .createEntryFromDocument(d);
        }

        catch (IOException e) {
            logger.warn(" Unable to calculate the median entry. ");
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
    }


    @Override
    public int getMarkerEntriesSize() throws LuceneAdaptorException {

        int numberOfEntries;
        Query epochRangeQuery = NumericRangeQuery.newLongRange(

                MarkerEntry.EPOCH_ID, 0l,
                Long.MAX_VALUE,
                true, true);

        TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
        searchDocumentsInLucene(epochRangeQuery, totalHitCountCollector);
        numberOfEntries = totalHitCountCollector.getTotalHits();

        return numberOfEntries;
    }
}
