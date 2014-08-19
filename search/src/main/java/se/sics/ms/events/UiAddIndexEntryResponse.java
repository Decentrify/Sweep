package se.sics.ms.events;

import se.sics.kompics.Event;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 5:49 PM
 */
public class UiAddIndexEntryResponse extends Event {
    private final boolean isSuccessful;

    public UiAddIndexEntryResponse(boolean successful) {
        isSuccessful = successful;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }
}
