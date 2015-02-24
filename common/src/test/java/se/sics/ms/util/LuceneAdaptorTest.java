package se.sics.ms.util;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.common.LuceneAdaptor;
import se.sics.ms.common.LuceneAdaptorException;
import se.sics.ms.common.LuceneAdaptorImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.IndexEntry;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for the Lucene Adapter.
 * Created by babbarshaer on 2015-02-24.
 */
public class LuceneAdaptorTest {
    
    private static Directory directory;
    private static IndexWriterConfig config;
    private static Logger logger = LoggerFactory.getLogger(LuceneAdaptorTest.class);
    private static LuceneAdaptor luceneAdaptor;
    
    @BeforeClass    
    public static void beforeClass() throws LuceneAdaptorException {
        
        // Initialize index writer entries.
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
        config = new IndexWriterConfig(Version.LUCENE_42,analyzer);
        directory = new RAMDirectory();
        
        // Initialize Lucene Adaptor.
        luceneAdaptor = new LuceneAdaptorImpl(directory,config);
        luceneAdaptor.initialEmptyWriterCommit();
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
        logger.info("Closing the index");
        directory.close();
    }

    @Before
    public void before(){
        
    }
    
    @After
    public void after(){
        
        try {
            logger.debug("Executing after test cleanup");
            luceneAdaptor.wipeIndexData();
        }
        catch (LuceneAdaptorException e) {
            logger.warn("Unable to wipe the data.");
            e.printStackTrace();
        }
    }

    @Test
    public void initialEmptyCommitTest() throws LuceneAdaptorException {
        logger.info(" Testing initial empty writer commit.");
        luceneAdaptor.initialEmptyWriterCommit();
    }
    
    @Test
    public void luceneInstanceSizeTest() throws LuceneAdaptorException {
        
        logger.info("Initiating Lucene Instance Size Test.");
        
        int _luceneInstanceSize = 10;
        List<IndexEntry> testEntries = _createJunkIndexEntry(_luceneInstanceSize);
        
        for(IndexEntry entry : testEntries){
            Document doc = _getDocumentForIndexEntry(entry);
            logger.debug(doc.toString());
            luceneAdaptor.addDocumentToLucene(doc);
        }
        
        Assert.assertEquals("Matching instance size", _luceneInstanceSize, luceneAdaptor.getSizeOfLuceneInstance());
    }
    
    
        

    /**
     * For testing reasons create a list of junk index entries.
     * @param numberOfIndexEntries number of index entries to create
     * @return
     */
    private List<IndexEntry> _createJunkIndexEntry(int numberOfIndexEntries){
        
        List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
        for(int i=0 ; i < numberOfIndexEntries ; i++){
            
            IndexEntry entry = new IndexEntry("global"+i, i, "url","file"+i, 0, null, null, MsConfig.Categories.Video,"description"+i, "hash"+i, null);
            indexEntries.add(entry);
        }
        return indexEntries;
    }


    /**
     * Construct lucene document instance for the Index Entry.
     * @param entry Index Entry
     * @return
     */
    private Document _getDocumentForIndexEntry(IndexEntry entry){
        
        Document doc = new Document();
        
        doc.add(new StringField(IndexEntry.GLOBAL_ID, entry.getGlobalId(), Field.Store.YES));
        doc.add(new LongField(IndexEntry.ID, entry.getId(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.URL, entry.getUrl()));
        doc.add(new TextField(IndexEntry.FILE_NAME, entry.getFileName(), Field.Store.YES));
        doc.add(new IntField(IndexEntry.CATEGORY, entry.getCategory().ordinal(), Field.Store.YES));
        doc.add(new TextField(IndexEntry.DESCRIPTION, entry.getDescription(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.HASH, entry.getHash()));
        if (entry.getLeaderId() == null)
            doc.add(new StringField(IndexEntry.LEADER_ID, new String(), Field.Store.YES));
        else
            doc.add(new StringField(IndexEntry.LEADER_ID, new BASE64Encoder().encode(entry.getLeaderId().getEncoded()), Field.Store.YES));

        if (entry.getFileSize() != 0) {
            doc.add(new LongField(IndexEntry.FILE_SIZE, entry.getFileSize(), Field.Store.YES));
        }

        if (entry.getUploaded() != null) {
            doc.add(new LongField(IndexEntry.UPLOADED, entry.getUploaded().getTime(),
                    Field.Store.YES));
        }

        if (entry.getLanguage() != null) {
            doc.add(new StringField(IndexEntry.LANGUAGE, entry.getLanguage(), Field.Store.YES));
        }
        
        return doc;
    }
    
    
    
}
