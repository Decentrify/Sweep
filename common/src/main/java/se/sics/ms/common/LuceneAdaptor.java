package se.sics.ms.common;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.*;
import se.sics.ms.types.IndexEntry;

import java.util.List;

/**
 * Interface for the Lucene Access.
 *
 * Created by babbarshaer on 2015-02-23.
 */
public interface LuceneAdaptor {

    /**
     * Adds document to the Lucene instance.
     *
     * @param d document to add in lucene.
     * @throws LuceneAdaptorException
     */
    public void addDocumentToLucene(Document d) throws LuceneAdaptorException;

    /**
     * Based on the supplied collector, return the search result.
     * 
     * @param query Search Query
     * @param collector Collector for results.
     * @return ScoreDocs
     * @throws LuceneAdaptorException
     */
    public void searchDocumentsInLucene(Query query, Collector collector) throws LuceneAdaptorException;


    /**
     * Search for documents in lucene based on the search query 
     * and sorting defined by used.
     *
     * @param query Search Query
     * @param sort Sorting defined by user
     * @param maxEntryReturnSize maximum entries to return.
     * @return ScoreDocs
     * @throws LuceneAdaptorException
     */
    public ScoreDoc[] searchDocumentsInLucene(Query query, Sort sort, int maxEntryReturnSize) throws LuceneAdaptorException;

    /**
     * Fetch the size of the lucene instance.
     * 
     * @return Size of lucene instance.
     * @throws LuceneAdaptorException
     */
    public int getSizeOfLuceneInstance() throws LuceneAdaptorException;


    /**
     * Remove the document from the Lucene.
     *
     * @param query Query used to identify documents to delete
     * @throws LuceneAdaptorException
     */
    public void deleteDocumentsFromLucene(Query query) throws LuceneAdaptorException;

    /**
     *  Remove the documents from Lucene.
     * @param query Query to delete document from lucene.
     * @throws LuceneAdaptorException
     */
    public void deleteDocumentsFromLucene(Query... query) throws LuceneAdaptorException;

    /**
     * Initial Empty Commit before index could be opened.
     */
    public void initialEmptyWriterCommit() throws LuceneAdaptorException;


    /**
     * Clean all the data inside the Lucene Instance.
     * @throws LuceneAdaptorException
     */
    public void wipeIndexData() throws LuceneAdaptorException;
}
