package search.system.peer.search;

import se.sics.gvod.timer.UUID;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import tman.system.peer.tman.LeaderRequest.AddIndexEntry;

/**
 * Superclass from the leader answering requests.
 */
public abstract class LeaderResponse extends Message {
	private static final long serialVersionUID = 8683160410473099622L;
	private final UUID uuid;

	/**
	 * @param source
	 *            the message source
	 * @param destination
	 *            the message destination
	 * @param uuid
	 *            the id of the request which is answered
	 */
	protected LeaderResponse(Address source, Address destination, UUID uuid) {
		super(source, destination);
		this.uuid = uuid;
	}

	/**
	 * @return the id of the request which is answered
	 */
	public UUID getUuid() {
		return uuid;
	}

	/**
	 * Response of the leader to {@link AddIndexEntry} requests.
	 */
	public static class IndexEntryAdded extends LeaderResponse {
		private static final long serialVersionUID = 1794144097056983843L;

		/**
		 * @param source
		 *            the message source
		 * @param destination
		 *            the message destination
		 * @param uuid
		 *            the id of the request which is answered
		 */
		protected IndexEntryAdded(Address source, Address destination, UUID uuid) {
			super(source, destination, uuid);
		}
	}
}
