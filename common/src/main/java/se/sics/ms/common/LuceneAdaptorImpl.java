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

import java.io.IOException;

/**
 * Implementation Class for the Lucene Adaptor.
 * Created by babbarshaer on 2015-02-23.
 */
public class LuceneAdaptorImpl implements LuceneAdaptor {

    private IndexWriterConfig config;
    private Directory directory;
    private Logger logger = LoggerFactory.getLogger(LuceneAdaptorImpl.class);

    public LuceneAdaptorImpl(Directory directory, IndexWriterConfig config) {
        this.directory = directory;
        this.config = config;
    }

    public IndexWriterConfig getConfig() {
        return this.config;
    }

    public Directory getDirectory() {
        return this.directory;
    }

    public void setConfig(IndexWriterConfig config) {
        this.config = config;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

    @Override
    public void addDocumentToLucene(Document d) throws LuceneAdaptorException {

        IndexWriter writer = null;
        try {
            writer = new IndexWriter(directory, config);
            writer.addDocument(d);
            writer.commit();

        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        } finally {
            silentlyCloseWriter(writer);
        }

    }

    @Override
    public ScoreDoc[] searchDocumentsInLucene(Query query, TopDocsCollector collector) throws LuceneAdaptorException {
        IndexReader reader = null;
        ScoreDoc[] scds = new ScoreDoc[0];
        try {
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.search(query, collector);

            scds = collector.topDocs().scoreDocs;

        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        } finally {

            silentlyCloseReader(reader);
        }

        return scds;
    }

    @Override
    public ScoreDoc[] searchDocumentsInLucene(Query query, Sort sort, int maxEntryReturnSize) throws LuceneAdaptorException {

        IndexReader reader = null;
        ScoreDoc[] scds = null;

        try {
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopFieldDocs topFieldDocs = searcher.search(query, maxEntryReturnSize, sort);
            scds = topFieldDocs.scoreDocs;

        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally {
            silentlyCloseReader(reader);
        }

        return scds;
    }

    @Override
    public int getSizeOfLuceneInstance() throws LuceneAdaptorException {
        int size = 0;

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(directory);
            size = reader.numDocs();

        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        } finally {
            silentlyCloseReader(reader);
        }
        return size;
    }

    
    @Override
    public void deleteDocumentsFromLucene(Query query) throws LuceneAdaptorException {
        
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(directory,config);
            writer.deleteDocuments(query);
            
        } catch (IOException e) {
            e.printStackTrace();
            throw new LuceneAdaptorException(e.getMessage());
        }
        finally{
            silentlyCloseWriter(writer);
        }

    }


    private void silentlyCloseReader(IndexReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warn("Unable to close IndexReader.");
                e.printStackTrace();
            }
        }
    }


    private void silentlyCloseWriter(IndexWriter writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.warn("Unable to close IndexWriter.");
                e.printStackTrace();
            }
        }
    }

}
