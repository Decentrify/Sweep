package se.sics.ms.common;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;

/**
 * Lucene Component acting as a wrapper over the Lucene Adapter.
 * 
 * Created by babbarshaer on 2015-02-23.
 */
public class LuceneComponent extends ComponentDefinition {

    private LuceneAdaptor luceneAdaptor;
    private Directory directory;
    private IndexWriterConfig config;
    private Logger logger = LoggerFactory.getLogger(LuceneComponent.class);
    
    public LuceneComponent(LuceneComponentInit luceneComponentInit){
        doInit(luceneComponentInit);
        subscribe(startHandler,control);
    }

    /**
     * Initialize the Lucene Component.
     * @param luceneComponentInit Initialize the components of lucene.
     */
    private void doInit(LuceneComponentInit luceneComponentInit) {
        this.directory = luceneComponentInit.getDirectory();
        this.config = luceneComponentInit.getConfig();
        this.luceneAdaptor = new IndexEntryLuceneAdaptorImpl(directory,config);
    }

    /**
     * Subscribe the start handler.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("Lucene Component Started ...");
        }
    };


}
