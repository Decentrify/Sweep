package se.sics.ms.util;

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.common.LuceneAdaptor;
import se.sics.ms.common.LuceneAdaptorException;
import se.sics.ms.types.IndexEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Container for the stateless queries used when interacting 
 * with lucene.s 
 *  
 * Created by babbarshaer on 2015-04-23.
 */
public class ApplicationLuceneQueries {

    private static Logger logger = LoggerFactory.getLogger(ApplicationLuceneQueries.class);

    /**
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     *
     * @param min   the inclusive minimum of the range
     * @param max   the inclusive maximum of the range
     * @param limit the maximal amount of entries to be returned
     * @return a list of the entries found
     * @throws java.io.IOException if Lucene errors occur
     */
    public static List<IndexEntry> findIdRange(LuceneAdaptor adaptor, long min, long max, int limit) throws IOException {

        List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
        try {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, min, max, true, true);
            indexEntries = adaptor.searchIndexEntriesInLucene(query, new Sort(new SortField(IndexEntry.ID, SortField.Type.LONG)), limit);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return indexEntries;
    }


    /**
     * Returns min id value stored in Lucene
     *
     * @return min Id value stored in Lucene
     */
    public static long getMinStoredIdFromLucene(LuceneAdaptor adaptor) throws LuceneAdaptorException {

        long minStoreId = 0;
        Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        int numofEntries = 1;

        Sort sort = new Sort(new SortField(IndexEntry.ID, SortField.Type.LONG));
        List<IndexEntry> indexEntries = adaptor.searchIndexEntriesInLucene(query, sort, numofEntries);

        if (indexEntries.size() == 1) {
            minStoreId = indexEntries.get(0).getId();
        }
        return minStoreId;
    }

    /**
     * Returns max id value stored in Lucene
     *
     * @return max Id value stored in Lucene
     */
    public static long getMaxStoredIdFromLucene(LuceneAdaptor adaptor) throws LuceneAdaptorException {

        long maxStoreId = 0;
        Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        int numofEntries = 1;
        Sort sort = new Sort(new SortField(IndexEntry.ID, SortField.Type.LONG, true));
        List<IndexEntry> indexEntries = adaptor.searchIndexEntriesInLucene(query, sort, numofEntries);

        if (indexEntries.size() == 1) {
            maxStoreId = indexEntries.get(0).getId();
        }
        return maxStoreId;
    }

    /**
     * Deletes all documents from the index with ids less or equal then id
     *
     * @param id
     * @param bottom
     * @param top
     * @return
     */
    public static void deleteDocumentsWithIdLessThen(LuceneAdaptor adaptor, long id, long bottom, long top) throws LuceneAdaptorException {

        if (bottom < top) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, id, true, true);
            adaptor.deleteDocumentsFromLucene(query);
        } else {
            if (id < bottom) {
                Query query1 = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, Long.MAX_VALUE - 1, true, true);
                Query query2 = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE + 1, id, true, true);
                adaptor.deleteDocumentsFromLucene(query1, query2);
            } else {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, id, true, true);
                adaptor.deleteDocumentsFromLucene(query);
            }
        }

    }

    /**
     * Deletes all documents from the index with ids bigger then id (not including)
     *
     * @param id
     * @param bottom
     * @param top
     */
    public static void deleteDocumentsWithIdMoreThen(LuceneAdaptor adaptor, long id, long bottom, long top) throws LuceneAdaptorException {


        if (bottom < top) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, top, true, true);
            adaptor.deleteDocumentsFromLucene(query);

        } else {
            if (id >= top) {
                Query query1 = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, Long.MAX_VALUE - 1, true, true);
                Query query2 = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE + 1, top, true, true);
                adaptor.deleteDocumentsFromLucene(query1, query2);
            } else {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, top, true, true);
                adaptor.deleteDocumentsFromLucene(query);
            }
        }
    }


    /**
     * Modify the exstingEntries set to remove the entries higher than median Id.
     *
     * @param medianId
     * @param existingEntries
     * @param including
     */
    public static void deleteHigherExistingEntries(Long medianId, Collection<Long> existingEntries, boolean including) {

        Iterator<Long> iterator = existingEntries.iterator();

        while (iterator.hasNext()) {
            Long currEntry = iterator.next();

            if (including) {
                if (currEntry.compareTo(medianId) >= 0)
                    iterator.remove();
            } else {
                if (currEntry.compareTo(medianId) > 0)
                    iterator.remove();
            }
        }
    }

    /**
     * Modify the existingEntries set to remove the entries lower than mediaId.
     *
     * @param medianId
     * @param existingEntries
     * @param including
     */
    public static void deleteLowerExistingEntries(Long medianId, Collection<Long> existingEntries, boolean including) {

        Iterator<Long> iterator = existingEntries.iterator();

        while (iterator.hasNext()) {
            Long currEntry = iterator.next();

            if (including) {
                if (currEntry.compareTo(medianId) <= 0)
                    iterator.remove();
            } else {
                if (currEntry.compareTo(medianId) < 0)
                    iterator.remove();
            }
        }
    }




}
