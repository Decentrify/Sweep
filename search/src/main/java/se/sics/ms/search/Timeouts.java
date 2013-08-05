package se.sics.ms.search;

import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.types.IndexEntry;

/**
 * Class that groups all timeouts used for by the {@link Search} class.
 */
public final class Timeouts {
	private Timeouts() {

	}

	/**
	 * Timeout for active {@link se.sics.peersearch.messages.SearchMessage.Request}s.
	 */
	protected static class SearchTimeout extends IndividualTimeout {

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 */
		public SearchTimeout(ScheduleTimeout request, int id) {
			super(request, id);
		}
	}

	/**
	 * Timeout for collecting {@link se.sics.peersearch.messages.RepairMessage.Response}s for a specific
	 * {@link se.sics.peersearch.messages.RepairMessage.Request}.
	 */
	protected static class ReplicationTimeout extends IndividualTimeout {

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 */
		public ReplicationTimeout(ScheduleTimeout request, int id) {
			super(request, id);
		}
	}

	/**
	 * Timeout for waiting for an {@link se.sics.peersearch.messages.AddIndexEntryMessage.Response} acknowledgment for an
	 * {@link se.sics.peersearch.messages.AddIndexEntryMessage.Response} request.
	 */
	protected static class AddRequestTimeout extends IndividualTimeout {
		private final int retryLimit;
		private int numberOfRetries = 0;
		private final IndexEntry entry;

		/**
         * @param request
         *            the ScheduleTimeout that holds the Timeout
         * @param retryLimit
         *            the number of retries for the related
         *            {@link se.sics.peersearch.messages.AddIndexEntryMessage.Request}
         * @param entry
*            the {@link se.sics.peersearch.types.IndexEntry} this timeout was scheduled for
         */
		public AddRequestTimeout(ScheduleTimeout request, int id, int retryLimit, IndexEntry entry) {
			super(request, id);
			this.retryLimit = retryLimit;
			this.entry = entry;
		}

		/**
		 * Increment the number of retries executed.
		 */
		public void incrementTries() {
			numberOfRetries++;
		}

		/**
		 * @return true if the number of retries exceeded the limit
		 */
		public boolean reachedRetryLimit() {
			return numberOfRetries > retryLimit;
		}

		/**
		 * @return the {@link IndexEntry} this timeout was scheduled for
		 */
		public IndexEntry getEntry() {
			return entry;
		}
	}

	/**
	 * Timeout used to delay the gap detection process after a gap in the local
	 * index store was found.
	 */
	protected static class GapTimeout extends IndividualTimeout {
		private final long indexId;

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 * @param id
		 *            the id of the suspected entry
		 */
		public GapTimeout(ScheduleTimeout request, int id, long indexId) {
			super(request, id);
			this.indexId = indexId;
		}

		/**
		 * @return the id of the suspected entry
		 */
		public long getIndexId() {
			return indexId;
		}
	}

	/**
	 * Periodic scheduled timeout event to garbage collect the recent request
	 * data structure of {@link Search}.
	 */
	protected static class RecentRequestsGcTimeout extends IndividualTimeout {

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 */
		public RecentRequestsGcTimeout(SchedulePeriodicTimeout request, int id) {
			super(request, id);
		}
	}
}
