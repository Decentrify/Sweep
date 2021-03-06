package se.sics.ms.util;

import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.common.ApplicationLuceneAdaptor;
import se.sics.ms.common.IndexEntryLuceneAdaptor;
import se.sics.ms.common.LuceneAdaptor;
import se.sics.ms.common.LuceneAdaptorException;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Container for the stateless queries used when interacting
 * with lucene.s
 * <p/>
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
    public static List<IndexEntry> findIdRange(IndexEntryLuceneAdaptor adaptor, long min, long max, int limit) throws IOException {

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
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     *
     * @param minId     the inclusive minimum of the range
     * @param maxId     the inclusive maximum of the range
     * @param collector Collector for limiting entries.
     * @return a list of the entries found
     * @throws java.io.IOException if Lucene errors occur
     */
    public static List<ApplicationEntry> findEntryIdRange(ApplicationLuceneAdaptor adaptor, ApplicationEntry.ApplicationEntryId minId, ApplicationEntry.ApplicationEntryId maxId, TopDocsCollector collector) {

        List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();

        try {

            BooleanQuery booleanQuery = new BooleanQuery();

            Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, minId.getEpochId(), maxId.getEpochId(), true, true);
            booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

            Query leaderQuery = NumericRangeQuery.newIntRange(ApplicationEntry.LEADER_ID, minId.getLeaderId(), maxId.getLeaderId(), true, true);
            booleanQuery.add(leaderQuery, BooleanClause.Occur.MUST);

            Query entryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, minId.getEntryId(), maxId.getEntryId(), true, true);
            booleanQuery.add(entryQuery, BooleanClause.Occur.MUST);

            entries = adaptor.searchApplicationEntriesInLucene(booleanQuery, collector);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return entries;
    }


    /**
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     *
     * @param minId     the inclusive minimum of the range
     * @param collector Collector for limiting entries.
     * @return a list of the entries found
     * @throws java.io.IOException if Lucene errors occur
     */
    public static List<ApplicationEntry> findEntryIdRange(ApplicationLuceneAdaptor adaptor, ApplicationEntry.ApplicationEntryId minId, TopDocsCollector collector) {

        List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();

        try {

            BooleanQuery booleanQuery = new BooleanQuery();

            Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, minId.getEpochId(), Long.MAX_VALUE, true, true);
            booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

            Query leaderQuery = NumericRangeQuery.newIntRange(ApplicationEntry.LEADER_ID, minId.getLeaderId(), Integer.MAX_VALUE, true, true);
            booleanQuery.add(leaderQuery, BooleanClause.Occur.MUST);

            Query entryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, minId.getEntryId(), Long.MAX_VALUE, true, true);
            booleanQuery.add(entryQuery, BooleanClause.Occur.MUST);

            entries = adaptor.searchApplicationEntriesInLucene(booleanQuery, collector);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return entries;
    }


    /**
     * Find the particular entry id in the local index in the system.
     *
     * @param adaptor adaptor
     * @param entryId entry
     * @return
     */
    public static ApplicationEntry findEntryId(ApplicationLuceneAdaptor adaptor, ApplicationEntry.ApplicationEntryId entryId) {

        ApplicationEntry entry = null;

        try {

            BooleanQuery booleanQuery = new BooleanQuery();

            Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, entryId.getEpochId(), entryId.getEpochId(), true, true);
            booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

            Query leaderQuery = NumericRangeQuery.newIntRange(ApplicationEntry.LEADER_ID, entryId.getLeaderId(), entryId.getLeaderId(), true, true);
            booleanQuery.add(leaderQuery, BooleanClause.Occur.MUST);

            Query entryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, entryId.getEntryId(), entryId.getEntryId(), true, true);
            booleanQuery.add(entryQuery, BooleanClause.Occur.MUST);

            Sort sort = new Sort(SortField.FIELD_SCORE,
                    new SortField(ApplicationEntry.EPOCH_ID, SortField.Type.LONG),
                    new SortField(ApplicationEntry.LEADER_ID, SortField.Type.INT),
                    new SortField(ApplicationEntry.ENTRY_ID, SortField.Type.LONG));

            List<ApplicationEntry> entries = adaptor.searchApplicationEntriesInLucene(booleanQuery, sort, 1);

            entry = entries.isEmpty() ? entry : entries.get(0);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return entry;


    }



    /**
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     * @deprecated
     *
     * @param minId     the inclusive minimum of the range
     * @param collector Collector for limiting entries.
     * @return a list of the entries found
     * @throws java.io.IOException if Lucene errors occur
     */
    public static List<ApplicationEntry> strictEntryIdRange(ApplicationLuceneAdaptor adaptor, ApplicationEntry.ApplicationEntryId minId, TopDocsCollector collector) {

        List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();

        try {

            BooleanQuery booleanQuery = new BooleanQuery();

            Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, minId.getEpochId(), minId.getEpochId(), true, true);
            booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

            Query leaderQuery = NumericRangeQuery.newIntRange(ApplicationEntry.LEADER_ID, minId.getLeaderId(), minId.getLeaderId(), true, true);
            booleanQuery.add(leaderQuery, BooleanClause.Occur.MUST);

            Query entryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, minId.getEntryId(), Long.MAX_VALUE, true, true);
            booleanQuery.add(entryQuery, BooleanClause.Occur.MUST);

            entries = adaptor.searchApplicationEntriesInLucene(booleanQuery, collector);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return entries;
    }


    /**
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     *
     * @param minId     the inclusive minimum of the range
     * @return a list of the entries found
     *
     */
    public static List<ApplicationEntry> strictEntryIdRangeOnDefaultSort(ApplicationLuceneAdaptor adaptor, ApplicationEntry.ApplicationEntryId minId, int maxCount) {

        List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();

        try {

            BooleanQuery booleanQuery = new BooleanQuery();

            Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, minId.getEpochId(), minId.getEpochId(), true, true);
            booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

            Query leaderQuery = NumericRangeQuery.newIntRange(ApplicationEntry.LEADER_ID, minId.getLeaderId(), minId.getLeaderId(), true, true);
            booleanQuery.add(leaderQuery, BooleanClause.Occur.MUST);

            Query entryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, minId.getEntryId(), Long.MAX_VALUE, true, true);
            booleanQuery.add(entryQuery, BooleanClause.Occur.MUST);


            Sort sort = new Sort(SortField.FIELD_SCORE,
                    new SortField(ApplicationEntry.EPOCH_ID, SortField.Type.LONG),
                    new SortField(ApplicationEntry.LEADER_ID, SortField.Type.INT),
                    new SortField(ApplicationEntry.ENTRY_ID, SortField.Type.LONG));

            entries = adaptor.searchApplicationEntriesInLucene(booleanQuery, sort, maxCount);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("Exception while trying to fetch the index entries between specified range.");
        }

        return entries;
    }




    /**
     * In case the node decides to initiate shard procedure, the splitting point needs to be calculated.
     * This splitting point happens to be midpoint of the sorted entries in the system.
     *
     * @param luceneAdaptor
     * @return
     */
    public static ApplicationEntry.ApplicationEntryId getMedianId(ApplicationLuceneAdaptor luceneAdaptor) throws LuceneAdaptorException {

        BooleanQuery booleanQuery = new BooleanQuery();

        Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, 0l, (long) Integer.MAX_VALUE, true, true);
        booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);


        Sort sort = new Sort(SortField.FIELD_SCORE,
                new SortField(ApplicationEntry.EPOCH_ID, SortField.Type.LONG),
                new SortField(ApplicationEntry.LEADER_ID, SortField.Type.INT),
                new SortField(ApplicationEntry.ENTRY_ID, SortField.Type.LONG));

        ApplicationEntry entry = luceneAdaptor.getMedianEntry(booleanQuery, sort, Integer.MAX_VALUE);
        return entry != null ? entry.getApplicationEntryId() : null;
    }


    /**
     * Returns min id value stored in Lucene
     *
     * @return min Id value stored in Lucene
     */
    public static long getMinStoredIdFromLucene(IndexEntryLuceneAdaptor adaptor) throws LuceneAdaptorException {

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
    public static long getMaxStoredIdFromLucene(IndexEntryLuceneAdaptor adaptor) throws LuceneAdaptorException {

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
     * Delete the documents with the median identifier which is
     * less than the specified medianId including the exact median id entry.
     *
     * As part of this method, we split at the leader unit level, so we do not split inside
     * the leader unit as that means extra meta data that needs to be held.
     *
     * @param luceneAdaptor Adaptor.
     * @param medianId MedianId.
     */
    public static void deleteDocumentsWithIdMoreThenMod(ApplicationLuceneAdaptor luceneAdaptor, ApplicationEntry.ApplicationEntryId medianId) throws LuceneAdaptorException {

        BooleanQuery query = new BooleanQuery();

        Long epochId = medianId.getEpochId();
        Integer leaderId = medianId.getLeaderId();

        Query epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, epochId, epochId, true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);

        Query leaderQuery = NumericRangeQuery.newIntRange( ApplicationEntry.LEADER_ID, leaderId, Integer.MAX_VALUE, true, true);
        query.add(leaderQuery, BooleanClause.Occur.MUST);

        luceneAdaptor.deleteDocumentsFromLucene(query); // Cleared the Epoch First.

        query = new BooleanQuery();

        epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, epochId, Long.MAX_VALUE, false , true);
        query.add(epochQuery, BooleanClause.Occur.MUST);

        luceneAdaptor.deleteDocumentsFromLucene(query);
    }


    /**
     * Deletes the document with the information which
     * is
     *
     * @param luceneAdaptor Lucene Adaptor.
     * @param medianId MedianId.
     */
    public static void deleteDocumentsWithIdLessThenMod(ApplicationLuceneAdaptor luceneAdaptor, ApplicationEntry.ApplicationEntryId medianId) throws LuceneAdaptorException {

        BooleanQuery query = new BooleanQuery();

        Long epochId = medianId.getEpochId();
        Integer leaderId = medianId.getLeaderId();

        Query epochQuery = NumericRangeQuery.newLongRange(ApplicationEntry.EPOCH_ID, Long.MIN_VALUE, epochId, true, false);
        query.add(epochQuery, BooleanClause.Occur.MUST);

        luceneAdaptor.deleteDocumentsFromLucene(query);
        
        query = new BooleanQuery();

        epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, epochId, epochId, true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);

        Query leaderQuery = NumericRangeQuery.newIntRange( ApplicationEntry.LEADER_ID, Integer.MIN_VALUE, leaderId, true, false );
        query.add(leaderQuery, BooleanClause.Occur.MUST);
        
        luceneAdaptor.deleteDocumentsFromLucene(query);
    }
    

    /**
     * Delete the documents with the median identifier which is 
     * less than the specified medianId.
     *
     * @param luceneAdaptor Adaptor.
     * @param medianId MedianId.
     */
    public static void deleteDocumentsWithIdMoreThen( ApplicationLuceneAdaptor luceneAdaptor, ApplicationEntry.ApplicationEntryId medianId ) throws LuceneAdaptorException {
        
        BooleanQuery query = new BooleanQuery();
        
        Long epochId = medianId.getEpochId();
        Integer leaderId = medianId.getLeaderId();
        
        
        // Delete the epoch completely first from that point ...
        
        Query epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, epochId, epochId, true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);
        
        Query leaderQuery = NumericRangeQuery.newIntRange( ApplicationEntry.LEADER_ID, leaderId, Integer.MAX_VALUE, true, true);
        query.add(leaderQuery, BooleanClause.Occur.MUST);
        
        Query entryIdQuery = NumericRangeQuery.newLongRange( ApplicationEntry.ENTRY_ID, 0l, 0l , true, true);
        query.add(entryIdQuery, BooleanClause.Occur.MUST_NOT);
        
        luceneAdaptor.deleteDocumentsFromLucene(query);
        
        
        // Delete the higher entries now from the next epoch ...
        
        query = new BooleanQuery();
        epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, epochId +1 , Long.MAX_VALUE, true, true);
        
        query.add(epochQuery, BooleanClause.Occur.MUST);
        query.add(entryIdQuery, BooleanClause.Occur.MUST_NOT);
        
        luceneAdaptor.deleteDocumentsFromLucene(query);
    }


    /**
     * Delete the documents with median Identifier that is  
     * more than the specified medianId. 
     * 
     * @param luceneAdaptor Lucene Adaptor.
     * @param medianId MedianId.
     */
    public static void deleteDocumentsWithIdLessThen(ApplicationLuceneAdaptor luceneAdaptor, ApplicationEntry.ApplicationEntryId medianId) throws LuceneAdaptorException {

        BooleanQuery query = new BooleanQuery();
        
        Long lowestEpochId = medianId.getEpochId();
        Integer leaderId = medianId.getLeaderId();
        
        Query epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, Long.MIN_VALUE, lowestEpochId - 1 , true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);
        
        Query entryIdQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, 0l, 0l, true, true);
        query.add(entryIdQuery, BooleanClause.Occur.MUST_NOT);
        
        luceneAdaptor.deleteDocumentsFromLucene(query);
        
        
        // ===== Delete till all the leader id in the lowest epoch.
        
        query = new BooleanQuery();

        epochQuery = NumericRangeQuery.newLongRange( ApplicationEntry.EPOCH_ID, lowestEpochId, lowestEpochId, true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);
        
        Query leaderQuery = NumericRangeQuery.newIntRange( ApplicationEntry.LEADER_ID, Integer.MIN_VALUE, leaderId, true, false );
        query.add(leaderQuery, BooleanClause.Occur.MUST);
        
        query.add(entryIdQuery, BooleanClause.Occur.MUST_NOT);
        luceneAdaptor.deleteDocumentsFromLucene(query);
        
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


    /**
     * Generate Query to help locate entries for a particular leader packet in lucene.
     *
     * @param epochId
     * @param leaderId
     * @return
     */
    public static Query entriesInLeaderPacketQuery(String epochString, long epochId, String leaderString, int leaderId) {

        BooleanQuery query = new BooleanQuery();

        Query epochQuery = NumericRangeQuery.newLongRange(epochString, epochId, epochId, true, true);
        query.add(epochQuery, BooleanClause.Occur.MUST);

        Query leaderQuery = NumericRangeQuery.newIntRange(leaderString, leaderId, leaderId, true, true);
        query.add(leaderQuery, BooleanClause.Occur.MUST);

        return query;
    }
}
