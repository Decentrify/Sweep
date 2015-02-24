package se.sics.ms.common;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import se.sics.kompics.Init;

/**
 * Created by babbarshaer on 2015-02-23.
 */
public class LuceneComponentInit extends Init<LuceneComponent> {

    private final Directory directory;
    private final IndexWriterConfig config;
    
    
    public LuceneComponentInit(Directory directory, IndexWriterConfig config){
        this.directory = directory;
        this.config = config;
    }

    public Directory getDirectory() {
        return directory;
    }

    public IndexWriterConfig getConfig() {
        return config;
    }
}
