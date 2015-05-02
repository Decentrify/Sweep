package se.sics.ms.data;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.store.Directory;
import se.sics.ms.common.LuceneAdaptorBasic;
import se.sics.ms.common.LuceneAdaptorException;

import java.util.List;

/**
 * Specific Adaptor Class for dealing with the Application Entries.
 *
 * Created by babbar on 2015-05-02.
 */
public abstract class ApplicationLuceneAdaptor extends LuceneAdaptorBasic {


    public ApplicationLuceneAdaptor(Directory directory, IndexWriterConfig config) {
        super(directory, config);
    }

    /**
     * Search for Index Entries in Lucene, based on the provided query and the collector instance.
     *
     * @param searchQuery query used to search.
     * @param collector Collector supplied for query.
     * @return List of Index Entries
     * @throws se.sics.ms.common.LuceneAdaptorException
     */
    public abstract List<ApplicationEntry> searchApplicationEntriesInLucene(Query searchQuery, TopDocsCollector collector) throws LuceneAdaptorException;


    /**
     * Search for Index Entries in Lucene, based on the provided query and the collector instance.
     *
     * @param searchQuery query used to search.
     * @param sort Sort defined for search
     * @param maxEntries maximum entries returned by Lucene.
     * @return List of Index Entries
     * @throws LuceneAdaptorException
     */
    public abstract List<ApplicationEntry> searchApplicationEntriesInLucene(Query searchQuery, Sort sort , int maxEntries) throws LuceneAdaptorException;



}
