package se.sics.ms.search;

import se.sics.kompics.Event;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 5:03 PM
 */
public class AddIndexEntryUiRequest extends Event {
    private final IndexEntry entry;

    public AddIndexEntryUiRequest(IndexEntry entry) {
        this.entry = entry;
    }

    public IndexEntry getEntry() {
        return entry;
    }
}
