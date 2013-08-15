package se.sics.ms.peer;

import se.sics.kompics.Event;
import se.sics.kompics.PortType;

import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

public class SimulationEventsPort extends PortType {
	{
		positive(AddIndexSimulated.class);
        positive(SearchSimulated.class);
	}

	public static final class AddIndexSimulated extends Event {
		private final IndexEntry entry;

		public AddIndexSimulated(IndexEntry entry) {
			this.entry = entry;
		}

		public IndexEntry getEntry() {
			return entry;
		}
	}

    public static final class SearchSimulated extends Event {
        private final SearchPattern searchPattern;

        public SearchSimulated(SearchPattern searchPattern) {
            this.searchPattern = searchPattern;
        }

        public SearchPattern getSearchPattern() {
            return searchPattern;
        }
    }
}
