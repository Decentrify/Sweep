package se.sics.ms.gradient.events;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/12/13
 * Time: 1:14 PM
 */
public class ViewSizeMessage {
    public static class Request extends Event {
        private final TimeoutId timeoutId;
        private final IndexEntry newEntry;
        private final VodAddress source;

        public Request(TimeoutId timeoutId, IndexEntry newEntry, VodAddress source) {
            this.timeoutId = timeoutId;
            this.newEntry = newEntry;
            this.source = source;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }

        public IndexEntry getNewEntry() {
            return newEntry;
        }

        public VodAddress getSource() {
            return source;
        }
    }

    public static class Response extends Event {
        private final TimeoutId timeoutId;
        private final IndexEntry newEntry;
        private final int viewSize;
        private final VodAddress source;

        public Response(TimeoutId timeoutId, IndexEntry newEntry, int viewSize, VodAddress source) {
            this.timeoutId = timeoutId;
            this.newEntry = newEntry;
            this.viewSize = viewSize;
            this.source = source;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }

        public IndexEntry getNewEntry() {
            return newEntry;
        }

        public int getViewSize() {
            return viewSize;
        }

        public VodAddress getSource() {
            return source;
        }
    }
}
