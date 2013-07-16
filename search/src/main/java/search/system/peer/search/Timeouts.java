package search.system.peer.search;

import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.types.IndexEntry;
import tman.system.peer.tman.LeaderRequest.AddIndexEntry;

/**
 * Class that groups all timeouts used for by the {@link Search} class.
 */
public final class Timeouts {
	private Timeouts() {

	}

	/**
	 * Timeout for active {@link SearchRequest}s.
	 */
	protected static class SearchTimeout extends Timeout {

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 */
		public SearchTimeout(ScheduleTimeout request) {
			super(request);
		}
	}

	/**
	 * Timeout for collecting {@link ReplicationConfirmation}s for a specific
	 * {@link Replicate} request.
	 */
	protected static class ReplicationTimeout extends Timeout {
		private final UUID requestId;

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 * @param requestId
		 *            the unique request id of the {@link Replicate} request for
		 *            which this timeout was scheduled
		 */
		public ReplicationTimeout(ScheduleTimeout request, UUID requestId) {
			super(request);
			this.requestId = requestId;
		}

		/**
		 * @return the unique request id of the {@link Replicate} request for
		 *         which this timeout was scheduled
		 */
		public UUID getRequestId() {
			return requestId;
		}
	}

	/**
	 * Timeout for waiting for an {@link IndexEntryAdded} acknowledgment for an
	 * {@link AddIndexEntry} request.
	 */
	protected static class AddRequestTimeout extends Timeout {
		private final int retryLimit;
		private int numberOfRetries = 0;
		private final IndexEntry entry;

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 * @param retryLimit
		 *            the number of retries for the related
		 *            {@link AddIndexEntry} request
		 * @param entry
		 *            the {@link IndexEntry} this timeout was scheduled for
		 */
		public AddRequestTimeout(ScheduleTimeout request, int retryLimit, IndexEntry entry) {
			super(request);
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
	protected static class GapTimeout extends Timeout {
		private final long id;

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 * @param id
		 *            the id of the suspected entry
		 */
		public GapTimeout(ScheduleTimeout request, long id) {
			super(request);
			this.id = id;
		}

		/**
		 * @return the id of the suspected entry
		 */
		public long getId() {
			return id;
		}
	}

	/**
	 * Timeout used by the leader after to collect {@link GapDetectionResponse}
	 * s.
	 */
	protected static class GapDetectionTimeout extends Timeout {
		private final long id;

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 * @param id
		 *            the id of the suspected entry
		 */
		public GapDetectionTimeout(ScheduleTimeout request, long id) {
			super(request);
			this.id = id;
		}

		/**
		 * @return the id of the suspected entry
		 */
		public long getId() {
			return id;
		}
	}

	/**
	 * Periodic scheduled timeout event to garbage collect the recent request
	 * data structure of {@link Search}.
	 */
	protected static class RecentRequestsGcTimeout extends Timeout {

		/**
		 * @param request
		 *            the ScheduleTimeout that holds the Timeout
		 */
		public RecentRequestsGcTimeout(SchedulePeriodicTimeout request) {
			super(request);
		}
	}
}
