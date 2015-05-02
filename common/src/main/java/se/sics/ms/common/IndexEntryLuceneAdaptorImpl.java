package se.sics.ms.common;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.IndexEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation Class for the Lucene Adaptor.
 * Created by babbarshaer on 2015-02-23.
 */
public class IndexEntryLuceneAdaptorImpl extends IndexEntryLuceneAdaptor {

    private Logger logger = LoggerFactory.getLogger(IndexEntryLuceneAdaptorImpl.class);

    public IndexEntryLuceneAdaptorImpl(Directory directory, IndexWriterConfig config) {
        super(directory, config);
    }


    @Override
    public List<IndexEntry> searchIndexEntriesInLucene(Query searchQuery, TopDocsCollector collector) throws LuceneAdaptorException {
        
        IndexReader reader = null;
        try{
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.search(searchQuery, collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            ArrayList<IndexEntry> result = new ArrayList<IndexEntry>();
            for (int i = 0; i < hits.length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                // Check to avoid duplicate index entries in the response.
                IndexEntry entry = IndexEntry.IndexEntryHelper.createIndexEntry(d);
                if(result.contains(entry))
                    continue;
                result.add(entry);
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseReader(reader);
        }
    }

    @Override
    public List<IndexEntry> searchIndexEntriesInLucene(Query searchQuery, Sort sort, int maxEntries) throws LuceneAdaptorException {
        IndexReader reader = null;
        try{
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs docs = searcher.search(searchQuery, maxEntries, sort);
            ScoreDoc[] hits = docs.scoreDocs;

            ArrayList<IndexEntry> result = new ArrayList<IndexEntry>();
            for (int i = 0; i < hits.length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                IndexEntry entry = IndexEntry.IndexEntryHelper.createIndexEntry(d);
                if(result.contains(entry))
                    continue;
                result.add(entry);
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseReader(reader);
        }
    }

}
