package search.system.peer.util;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.IndexEntry;

import java.io.IOException;

/**
 * Test Class for Apache Lucene.
 * Created by babbarshaer on 2015-02-18.
 */
public class LuceneTest {

    private static Directory index;
    private static IndexWriterConfig indexWriterConfig;

    private static final String FILENAME = "filename";
    private static final String EPOCH = "epoch";
    private static final String LEADER = "leader";
    private static final String COUNTER = "counter";
    private final int _defaultEpoch = 10;
    private final long _defaultLeaderId = 9999;

    private static Logger logger = LoggerFactory.getLogger(LuceneTest.class);

    public LuceneTest() {

    }

    @BeforeClass
    public static void setUpClass() throws IOException {

        logger.warn(" Setting Up Lucene .. ");

        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
        indexWriterConfig = new IndexWriterConfig(Version.LUCENE_42, analyzer);
        index = new RAMDirectory();
        logger.info(" Lucene Setup Complete .. ");

        logger.info(" IndexWriter empty commit before test ... ");
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(index, indexWriterConfig);
            logger.info(" Empty commit complete and writer ready ...");
        } finally {

            if (writer != null) {
                writer.commit();
                writer.close();
            }
        }
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {


    }

    @After
    public void tearDown() throws IOException {

        IndexWriter writer = null;
        try {
            writer = new IndexWriter(index, indexWriterConfig);
            writer.deleteAll();
            logger.info("Clearing lucene index for next experiment. ");
        } finally {
            if (writer != null) {
                writer.commit();
                writer.close();
            }
        }

    }

    @Test
    public void testResultLength() throws IOException {

        createFixedFormatIndexEntryTestData(_defaultEpoch, _defaultLeaderId, 0, 2);

        //Read From the Index.
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            int maxDoc = reader.maxDoc();
            Assert.assertEquals("Asserting Total Entries.", new Integer(2), new Integer(maxDoc));
        } finally {
            if (reader != null)
                reader.close();
        }
    }


    @Test
    public void testBooleanSearchQuery() throws IOException {

        createFixedFormatIndexEntryTestData(_defaultEpoch, _defaultLeaderId, 0, 2);

        // =============== Construct a Boolean Query.
        BooleanQuery booleanQuery = new BooleanQuery();

        NumericRangeQuery<Integer> epochQuery = NumericRangeQuery.newIntRange(EPOCH, _defaultEpoch, _defaultEpoch, true, true);
        booleanQuery.add(epochQuery, BooleanClause.Occur.MUST);

        NumericRangeQuery<Integer> counterQuery = NumericRangeQuery.newIntRange(COUNTER, 0, 0, true, true);
        booleanQuery.add(counterQuery, BooleanClause.Occur.MUST);


        // ========== Read From the Index and Assertions.
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopScoreDocCollector collector = TopScoreDocCollector.create(25, true);
            searcher.search(booleanQuery, collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            Assert.assertNotNull(hits);
            Assert.assertEquals("Asserting Hits Length", new Integer(1), new Integer(hits.length));

            Document docRead = searcher.doc(hits[0].doc);
            Assert.assertEquals("Assert Epoch Match", new Integer(_defaultEpoch), Integer.valueOf(docRead.get(EPOCH)));
            Assert.assertEquals("Assert Counter Match", new Integer(0), Integer.valueOf(docRead.get(COUNTER)));
        } finally {
            if (reader != null)
                reader.close();
        }
    }


    @Test
    public void testBooleanDeleteQuery() throws IOException {

        createFixedFormatIndexEntryTestData(_defaultEpoch, _defaultLeaderId, 0, 2);
        createFixedFormatIndexEntryTestData(_defaultEpoch + 1, _defaultLeaderId, 0, 2);

        BooleanQuery booleanDeleteQuery = new BooleanQuery();

        Query epochRangeQuery = NumericRangeQuery.newIntRange(EPOCH, _defaultEpoch, _defaultEpoch + 1, true, true);
        booleanDeleteQuery.add(epochRangeQuery, BooleanClause.Occur.SHOULD);

        Query landingEntryPrevention = NumericRangeQuery.newIntRange(COUNTER, 0, 0, true, true);
        booleanDeleteQuery.add(landingEntryPrevention, BooleanClause.Occur.MUST_NOT);

        IndexWriter writer = null;

        try {
            writer = new IndexWriter(index, indexWriterConfig);
            writer.deleteDocuments(booleanDeleteQuery);

        } finally {

            if (writer != null) {
                writer.commit();
                writer.close();
            }
        }

        logger.info("Required entries deletion complete.");


        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            int numEntries = reader.maxDoc();
            Assert.assertEquals("Comparing number of entries left after delete.", new Integer(2), new Integer(numEntries));

            int num = reader.numDocs();
            for (int i = 0; i < num; i++) {
                
                Document d = reader.document(i);
                Assert.assertEquals("Comparing the counter",new Integer(0), new Integer(d.get(COUNTER)));
                
            }

        } finally {
            if (reader != null)
                reader.close();
        }
    }

    /**
     * Important test scenario, which tests the deletion pattern 
     * during the partitioning.
     * 
     * ============================================================================
     *  
     *         epoch1               epoch2
     * | ~~~~ Entries ~~~~~~ | ~~~~~~ Entries ~~~~~~ | ~~~~~~~~~~ Entries ~~~~~~~ |
     *  
     * ===========================================================================
     * 
     * Step1 : Identify the middle entry by taking the half of the total.
     * Step2 : Clear of the entries to the left or the right of the middle entry.
     * Step3 : When clearing the entries, leave the landing entries intact.
     * STep4 : Test the remaining entries.
     */
    @Test
    public void testPartitioningDeleteScenario() throws IOException {
        
        createFixedFormatIndexEntryTestData(_defaultEpoch, _defaultLeaderId, 0, 10);
        createFixedFormatIndexEntryTestData(_defaultEpoch+2, _defaultLeaderId+2, 0, 5);
        createFixedFormatIndexEntryTestData(_defaultEpoch+1, _defaultLeaderId+1, 0, 5);
        
        
        // Epochs = 10,11,12;
        // Number of Entries = 20;
        // 3 leaders -> Distinct Landing entries = 3;
        
        logger.info("Before partitioning range query created.");
        
        // Epoch Range - |Start Epoch| - |My Epoch|. - In Normal Case Start Epoch always 0.
        Query wholePartitionSearchQuery = NumericRangeQuery.newIntRange(EPOCH, 0, 15, true, true);
        
        logger.info(" Going to read number of entries. ");
        IndexReader reader = null;
        try{
            reader = DirectoryReader.open(index);
            int numDocs = reader.numDocs();
            
            
            Double d = Math.ceil(numDocs/2);
            int half = d.intValue();

            SortField epochStoreField = new SortField(EPOCH, SortField.Type.INT);
            SortField counterStoreField = new SortField(COUNTER, SortField.Type.INT);
            Sort sort = new Sort(epochStoreField,counterStoreField);

            logger.info("Sort Order Defined.");
            IndexSearcher searcher  = new IndexSearcher(reader);
            
            // 3000 = number of results to return. (In sweep, max partitioning size.)
            TopFieldDocs topFieldDocs = searcher.search(wholePartitionSearchQuery, 3000 , sort);
            ScoreDoc[] sds = topFieldDocs.scoreDocs;
            

            int middleEntryDocID = sds[half].doc;
            Document middleEntryDoc = searcher.doc(middleEntryDocID);
            
            int middleEntryEpoch = Integer.valueOf(middleEntryDoc.get(EPOCH));
            int middleEntryCounter = Integer.valueOf(middleEntryDoc.get(COUNTER));
            
            Assert.assertEquals("Middle Entry EPOCH Comparison", new Integer(11), new Integer(middleEntryEpoch));
            Assert.assertEquals("Middle Entry COUNTER Comparison", new Integer(0), new Integer(middleEntryCounter));
            
            // == INFORMATION.
            
            // Once we have this information, the leader knowing that the middle entry is what,
            // can direct other nodes in the leader group to perform partitioning if a particular fashion.
            
            // Only the information regarding the epoch and counter needs to be disseminated to the leader group.
            // nothing else needs to be transferred to the leader group.

        }
        finally {
            if(reader != null)
                reader.close();
        }
    }
    
    
    
    
    
    
    
    


    /**
     * Create Junk Index Entries in a predefined format.
     * In this scenario, the junk entries have same EPOCH and LEADER_ID but different monotonic_counter.
     *
     * @param start
     * @param numberOfEntries
     */
    private void createFixedFormatIndexEntryTestData(int epoch, long leaderId, int start, int numberOfEntries) throws IOException {

        int end = start + numberOfEntries;
        Document doc;

        for (int curr = start; curr < end; curr++) {

            IndexWriter writer = new IndexWriter(index, indexWriterConfig);
            try {

                doc = new Document();
                doc.add(new StringField(FILENAME, "document" + curr, Field.Store.YES));
                doc.add(new IntField(EPOCH, epoch, Field.Store.YES));
                doc.add(new LongField(LEADER, leaderId, Field.Store.YES));
                doc.add(new IntField(COUNTER, curr, Field.Store.YES));

                writer.addDocument(doc);

            } finally {
                writer.commit();
                writer.close();
            }
        }
    }

}
