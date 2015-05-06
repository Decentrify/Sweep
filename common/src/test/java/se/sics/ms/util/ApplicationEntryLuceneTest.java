package se.sics.ms.util;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.common.LuceneAdaptorException;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.data.ApplicationLuceneAdaptor;
import se.sics.ms.data.ApplicationLuceneAdaptorImpl;
import se.sics.ms.types.IndexEntry;

import java.io.IOException;
import java.util.List;

/**
 * Lucene Test for the Application Entry creation and searching in lucene
 * index.
 *
 * @author babbar
 */
public class ApplicationEntryLuceneTest {

    private static Directory directory;
    private static IndexWriterConfig config;
    private static Logger logger = LoggerFactory.getLogger(ApplicationEntryLuceneTest.class);
    private static ApplicationLuceneAdaptor luceneAdaptor;

    @BeforeClass
    public static void beforeClass() throws LuceneAdaptorException {

        logger.info("Setting up the Lucene Instance.");

        // Initialize index writer entries.
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
        config = new IndexWriterConfig(Version.LUCENE_42,analyzer);
        directory = new RAMDirectory();

        logger.info("Initializing the Lucene Adaptor");

        // Initialize Lucene Adaptor.
        luceneAdaptor = new ApplicationLuceneAdaptorImpl(directory,config);
        luceneAdaptor.initialEmptyWriterCommit();

        logger.info("Lucene Ready for adding data.");
    }


    @AfterClass
    public static void tearDown() throws IOException {

        IndexWriter writer = null;
        try {
            writer = new IndexWriter(directory, config);
            writer.deleteAll();
            logger.info("Clearing lucene index for next experiment. ");
        }
        finally {

            if (writer != null) {
                writer.commit();
                writer.close();
                directory.close();
            }
        }
    }


    @Test
    public void testLandingApplicationEntryAddition() throws LuceneAdaptorException {


        logger.info("  Simple Landing Entry Addition Test.");

        long epochId, entryId;
        epochId = entryId = 0;

        int leaderId = 0;
        ApplicationEntry originalEntry = createDefaultApplicationEntry(epochId, leaderId, entryId);

        Document doc = new Document();
        doc = ApplicationEntry.ApplicationEntryHelper.createDocumentFromEntry(doc, originalEntry);
        luceneAdaptor.addDocumentToLucene(doc);

        NumericRangeQuery<Long> landingEntryQuery = NumericRangeQuery.newLongRange(ApplicationEntry.ENTRY_ID, entryId, entryId, true, true);
        TopScoreDocCollector collector = TopScoreDocCollector.create(25, true);
        List<ApplicationEntry> entryList = luceneAdaptor.searchApplicationEntriesInLucene(landingEntryQuery, collector);

        Assert.assertEquals("Size Check", 1, entryList.size());
        Assert.assertEquals("Instance Check", IndexEntry.DEFAULT_ENTRY, entryList.get(0).getEntry());
    }


    /**
     * Create a test landing entry.
     * @param epochId epoch
     * @param leaderId leader
     * @return Entry
     */
    public ApplicationEntry createDefaultApplicationEntry (long epochId, int leaderId, long entryId){
        ApplicationEntry.ApplicationEntryId applicationEntryId = new ApplicationEntry.ApplicationEntryId(epochId, leaderId, entryId);
        return new ApplicationEntry(applicationEntryId);
    }


}
