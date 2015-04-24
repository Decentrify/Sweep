package se.sics.ms.events.simEvents;

import se.sics.ms.types.IndexEntry;

/**
 * Created by babbarshaer on 2015-03-01.
 */
public class AddIndexEntryP2pSimulated {

    private final IndexEntry entry;

    public AddIndexEntryP2pSimulated(IndexEntry entry){
        this.entry = entry;
    }

    public IndexEntry getIndexEntry() {
        return this.entry;
    }
}
