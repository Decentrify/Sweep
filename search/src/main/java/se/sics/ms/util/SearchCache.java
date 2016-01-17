package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.SearchPattern;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Cache encapsulation for the Search protocol.
 * This class holds and serves the search patterns
 * and the hits for the search query.
 *
 * Created by babbar on 2015-07-17.
 */
public class SearchCache {

    private Logger logger = LoggerFactory.getLogger(SearchCache.class);
    //<pattern, <srcId, <srcAdr, scoreList>>>
    private Map<SearchPattern, Map<Identifier, Pair<KAddress, List<IdScorePair>>>> queryResponseCache = new HashMap<>();

    public SearchCache(){
        this.logger.trace("Search Cache Created.");
    }

    /**
     * For a particular query consisting of a specific pattern,
     * store the matched and sorted documents.
     *
     * @param filePattern pattern
     * @param scorePairCollection collection
     */
    public void cachePattern(SearchPattern filePattern, Map<Identifier, Pair<KAddress, List<IdScorePair>>> scorePairCollection){
        queryResponseCache.put(filePattern, scorePairCollection);
    }

    /**
     * Fetch the score id collection based on the search pattern
     * provided by the client.
     *
     * @param pattern search pattern
     * @return hit - score/id collection.
     */
    public Map<Identifier, Pair<KAddress, List<IdScorePair>>> getScorePairCollection(SearchPattern pattern){
        return this.queryResponseCache.get(pattern);
    }

    /**
     * Each pattern is cached for a specific time by the application.
     * Remove the cached pattern when application informs
     * about cache time being over.
     *
     * @param pattern file pattern
     */
    public void removeCachedPattern(SearchPattern pattern){
        this.queryResponseCache.remove(pattern);
    }

}
