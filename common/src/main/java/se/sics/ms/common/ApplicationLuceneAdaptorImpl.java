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
import se.sics.ms.util.IdScorePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the class for the
 * Created by babbar on 2015-05-02.
 */
public class ApplicationLuceneAdaptorImpl extends ApplicationLuceneAdaptor {


    public ApplicationLuceneAdaptorImpl(Directory directory, IndexWriterConfig config) {
        super(directory, config);
    }
    private Logger logger = LoggerFactory.getLogger(ApplicationLuceneAdaptor.class);

    @Override
    public List<ApplicationEntry> searchApplicationEntriesInLucene(Query searchQuery, TopDocsCollector collector) throws LuceneAdaptorException {

        IndexReader reader = null;
        try{
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.search(searchQuery, collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            ArrayList<ApplicationEntry> result = new ArrayList<ApplicationEntry>();
            for (int i = 0; i < hits.length; ++i) {

                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                ApplicationEntry entry = ApplicationEntry.ApplicationEntryHelper.createApplicationEntryFromDocument(d);
                if(result.contains(entry))
                    continue;
                result.add(entry);
            }
            return result;
        }

        catch (IOException e) {

            logger.warn("Unable to search for application entries in Lucene.");
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseReader(reader);
        }

    }

    @Override
    public List<ApplicationEntry> searchApplicationEntriesInLucene(Query searchQuery, Sort sort, int maxEntries) throws LuceneAdaptorException {

        IndexReader reader = null;
        try{
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs docs = searcher.search(searchQuery, maxEntries, sort);
            ScoreDoc[] hits = docs.scoreDocs;

            ArrayList<ApplicationEntry> result = new ArrayList<ApplicationEntry>();
            for (int i = 0; i < hits.length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                ApplicationEntry entry = ApplicationEntry.ApplicationEntryHelper.createApplicationEntryFromDocument(d);
                if(result.contains(entry))
                    continue;
                result.add(entry);
            }
            return result;
        } catch (IOException e) {

            logger.warn("Unable to search for Application Entries in Lucene.");
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseReader(reader);
        }
    }

    @Override
    public ApplicationEntry getMedianEntry(Query searchQuery, Sort sort, int maxEntries) throws LuceneAdaptorException {
        
        try{
            
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs docs = searcher.search(searchQuery, maxEntries, sort);
            ScoreDoc[] hits = docs.scoreDocs;
            
            if(hits.length == 0){
                
                logger.warn("No Entries Found for the specified query.");
                return null;
            }
            
            int median = ( hits.length / 2 );
            Document d = searcher.doc( hits[median].doc );
            
            return ApplicationEntry.ApplicationEntryHelper
                    .createApplicationEntryFromDocument(d);
        } 
        
        catch (IOException e) {
            logger.warn(" Unable to calculate the median entry. ");
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
    }

    @Override
    public int getActualSizeOfInstance() throws LuceneAdaptorException {

        int size;
        BooleanQuery searchQuery = new BooleanQuery();

        Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        searchQuery.add(epochQuery, BooleanClause.Occur.MUST);
        
        Query entryIdQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, 0l, 0l, true, true);
        searchQuery.add(entryIdQuery, BooleanClause.Occur.MUST_NOT);

        
        TotalHitCountCollector countCollector = new TotalHitCountCollector();
        searchDocumentsInLucene(searchQuery, countCollector);
        
        size = countCollector.getTotalHits();
        return size;
    }

    @Override
    public int getApplicationEntrySize() throws LuceneAdaptorException {

        int numberOfEntries;
        Query epochRangeQuery = NumericRangeQuery.newLongRange(

                ApplicationEntry.EPOCH_ID, 0l,
                Long.MAX_VALUE,
                true, true);

        TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
        searchDocumentsInLucene(epochRangeQuery, totalHitCountCollector);
        numberOfEntries = totalHitCountCollector.getTotalHits();

        return numberOfEntries;
    }

    @Override
    public List<IdScorePair> getIdScoreCollection(Query searchQuery, TopDocsCollector collector) throws LuceneAdaptorException {


        IndexReader reader = null;
        List<IdScorePair> idScorePairList = new ArrayList<IdScorePair>();

        try{
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.search(searchQuery, collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            for (ScoreDoc hit : hits) {

                int docId = hit.doc;
                Document d = searcher.doc(docId);
                ApplicationEntry entry = ApplicationEntry.ApplicationEntryHelper.createApplicationEntryFromDocument(d);
                IdScorePair idScorePair = new IdScorePair(entry.getApplicationEntryId(), hit.score);
                if (idScorePairList.contains(idScorePair))
                    continue;
                idScorePairList.add(idScorePair);
            }
            return idScorePairList;
        }

        catch (IOException e) {

            logger.warn("Unable to search for application entries in Lucene.");
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseReader(reader);
        }
    }

}
