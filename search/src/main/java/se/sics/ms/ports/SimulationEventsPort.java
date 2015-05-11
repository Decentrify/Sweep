package se.sics.ms.ports;

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
        private final Integer searchTimeout;
        private final Integer searchParallelism;
        
        public SearchSimulated(SearchPattern searchPattern, Integer searchTimeout, Integer searchParallelism) {
            this.searchPattern = searchPattern;
            this.searchTimeout = searchTimeout;
            this.searchParallelism = searchParallelism;
        }

        public Integer getSearchTimeout() {
            return searchTimeout;
        }

        public Integer getSearchParallelism() {
            return searchParallelism;
        }

        public SearchPattern getSearchPattern() {
            return searchPattern;
        }
    }
}
