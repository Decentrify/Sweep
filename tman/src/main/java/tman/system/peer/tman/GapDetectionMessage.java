package tman.system.peer.tman;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * Superclass for messages used in the gap detection process.
 */
public abstract class GapDetectionMessage extends Message {
	private static final long serialVersionUID = -6614409671029387450L;
	private final int id;

	/**
	 * @param source
	 *            the source of the message
	 * @param destination
	 *            the destination of the message
	 * @param id
	 *            the id of the suspected entry
	 */
	public GapDetectionMessage(Address source, Address destination, int id) {
		super(source, destination);
		this.id = id;
	}

	/**
	 * @return the id of the suspected entry
	 */
	public int getId() {
		return id;
	}

	/**
	 * Request sent by the leader to detect gaps and randomly forwarded by the
	 * nodes.
	 */
	public static class GapDetectionRequest extends GapDetectionMessage {
		private static final long serialVersionUID = -6781734234125277256L;
		private int ttl;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param id
		 *            the id of the suspected entry
		 * @param ttl
		 *            the amount of hops this message is forwarded
		 */
		public GapDetectionRequest(Address source, Address destination, int id, int ttl) {
			super(source, destination, id);
			this.ttl = ttl;
		}

		/**
		 * @return the amount of hops this message should be forwarded further
		 */
		public int getTtl() {
			return ttl;
		}

		/**
		 * @param ttl
		 *            the amount of hops this message should be forwarded
		 *            further
		 */
		public void setTtl(int ttl) {
			this.ttl = ttl;
		}

		/**
		 * Decrement the amount of hops this message should be forwarded
		 * further.
		 */
		public void decrementTtl() {
			ttl--;
		}
	}

	/**
	 * Response to a {@link GapDetectionRequest} from the leader.
	 */
	public static class GapDetectionResponse extends GapDetectionMessage {
		private static final long serialVersionUID = 6775862180910763118L;
		private final boolean gap;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param id
		 *            the id of the suspected entry
		 * @param gap
		 *            true if an entry with this id does not exist locally
		 */
		public GapDetectionResponse(Address source, Address destination, int id, boolean gap) {
			super(source, destination, id);
			this.gap = gap;
		}

		/**
		 * @return true if the sender does not have an entry with the id
		 */
		public boolean isGap() {
			return gap;
		}
	}
}
