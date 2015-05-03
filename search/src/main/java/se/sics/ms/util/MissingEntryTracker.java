package se.sics.ms.util;

import org.javatuples.*;
import org.javatuples.Pair;
import se.sics.ms.types.EpochUpdate;

import java.util.HashMap;
import java.util.Map;

/**
 * Entry tracker for the smallest missing entry in the application.
 *
 * Created by babbar on 2015-04-27.
 */
public class MissingEntryTracker {

    private EpochUpdate currentEpochUpdate;
    private Map<Long, org.javatuples.Pair<Integer, Long>> entriesAdded;


    public MissingEntryTracker(){
        entriesAdded = new HashMap<Long, Pair<Integer, Long>>();
    }

    public void trackEpochUpdate(EpochUpdate epochUpdate){
        this.currentEpochUpdate = epochUpdate;
    }

    public void updateEntriesAndClose(long numEntries){
        this.currentEpochUpdate.setEntriesAndCloseUpdate(numEntries);
    }
}
