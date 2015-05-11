package se.sics.ms.ports;

import se.sics.kompics.Event;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PortType;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

public class SimulationEventsPort extends PortType {
	{
		positive(AddIndexSimulated.class);
        positive(SearchSimulated.Request.class);
        negative(SearchSimulated.Response.class);
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

    public static class SearchSimulated {
        
        
        public static class Request implements KompicsEvent {

            private final SearchPattern searchPattern;
            private final Integer searchTimeout;
            private final Integer searchParallelism;

            public Request(SearchPattern searchPattern, Integer searchTimeout, Integer searchParallelism) {
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
        
        
        public static class Response implements KompicsEvent {
            
            public int responses;
            public int partitionHit;

            public Response(int responses, int partitionHit){
                this.responses = responses;
                this.partitionHit = partitionHit;
            }
            
            public int getResponses() {
                return responses;
            }

            public int getPartitionHit() {
                return partitionHit;
            }
        }
        
        
    }
}
