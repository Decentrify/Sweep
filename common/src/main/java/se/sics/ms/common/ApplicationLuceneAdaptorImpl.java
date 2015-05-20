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
            
            int median = ( hits.length/2 );
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
    
}
