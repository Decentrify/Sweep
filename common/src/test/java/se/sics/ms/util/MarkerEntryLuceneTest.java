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
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.common.*;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.MarkerEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene Test for the Application Entry creation and searching in lucene
 * index.
 *
 * @author babbar
 */
public class MarkerEntryLuceneTest {

    private static Directory directory;
    private static IndexWriterConfig config;
    private static Logger logger = LoggerFactory.getLogger(MarkerEntryLuceneTest.class);
    private static MarkerEntryLuceneAdaptor luceneAdaptor;

    @BeforeClass
    public static void beforeClass() throws LuceneAdaptorException {

        logger.info("Setting up the Lucene Instance.");

        // Initialize index writer entries.
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
        config = new IndexWriterConfig(Version.LUCENE_42,analyzer);
        directory = new RAMDirectory();

        logger.info("Initializing the Lucene Adaptor");

        // Initialize Lucene Adaptor.
        luceneAdaptor = new MarkerEntryLuceneAdaptorImpl(directory,config);
        luceneAdaptor.initialEmptyWriterCommit();

        logger.info("Lucene Ready for adding data.");
    }


    @After
    public void tearDown() throws IOException {

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
            }
        }
    }
    
    @AfterClass
    public static void afterClass() throws IOException {
        directory.close();
    }

    
    @Test
    public void markerEntryAddition() throws LuceneAdaptorException {

        MarkerEntry entry = new MarkerEntry(0, 100);
        Document d = MarkerEntry.MarkerEntryHelper.createDocumentFromEntry(entry);
        luceneAdaptor.addDocumentToLucene(d);
        
        Assert.assertEquals("Size of Lucene Instance", 1, luceneAdaptor.getSizeOfLuceneInstance());
    }
    

    @Test
    public void lastMarkerEntryTest() throws LuceneAdaptorException {
        
        MarkerEntry baseEntry = new MarkerEntry(0, 100);
        MarkerEntry nextEntry = new MarkerEntry(0, 200);
        MarkerEntry highestEntry = new MarkerEntry(1, 29);
        
        List<MarkerEntry> entries = new ArrayList<MarkerEntry>();
        entries.add(baseEntry);
        entries.add(nextEntry);
        entries.add(highestEntry);
        
        
        for(MarkerEntry entry : entries) {
            
            Document d = MarkerEntry.MarkerEntryHelper.createDocumentFromEntry(entry);
            luceneAdaptor.addDocumentToLucene(d);
        }

        MarkerEntry lastEntry = luceneAdaptor.getLastEntry();
        Assert.assertEquals("Highest Marker Entry Test", highestEntry, lastEntry);
    }
    
    
}
