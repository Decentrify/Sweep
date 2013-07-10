package tman.system.peer.tman;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.peersearch.messages.IndexDisseminationMessage;
import se.sics.peersearch.messages.IndexRequestMessage;
import se.sics.peersearch.messages.StartIndexRequestMessage;

/**
 * This class acts as a port, handling all the messages that are involved in the
 * handling of index id requests and dissemination
 */
public class IndexRoutingPort extends PortType {
	{
		negative(IndexEvent.class);
		positive(IndexEvent.class);
        negative(StartIndexRequestMessage.class);
        positive(StartIndexRequestMessage.class);
        negative(IndexRequestMessage.class);
        positive(IndexRequestMessage.class);
        negative(StartIndexRequestMessage.class);
        positive(StartIndexRequestMessage.class);
        positive(IndexDisseminationMessage.class);
        negative(IndexDisseminationMessage.class);
	}

	/**
	 * A routing message for events
	 */
	public static class IndexMessage extends Message {
		private static final long serialVersionUID = -126793563227868661L;
		private final Event event;

		/**
		 * Constructor without destination address
		 * 
		 * @param event
		 *            the event that is to be routed
		 * @param source
		 *            the source of the message
		 */
		public IndexMessage(Event event, Address source) {
			super(source, null);
			this.event = event;
		}

		/**
		 * Constructor with destination address
		 * 
		 * @param event
		 *            the event that is to be routed
		 * @param source
		 *            the source address
		 * @param destination
		 *            the destination address
		 */
		public IndexMessage(Event event, Address source, Address destination) {
			super(source, destination);
			this.event = event;
		}

		/**
		 * Getter for the event
		 * 
		 * @return the event
		 */
		public Event getEvent() {
			return event;
		}
	}

	/**
	 * Message sent back to a node that has requested to get the index id
	 */
	public static class IndexResponseMessage extends Event {
		private static final long serialVersionUID = 6251581501397927390L;
		private final long index;
		private final UUID messageId;
        private final VodAddress source;
        private final VodAddress destination;

		/**
		 * Default constructor
		 * 
		 * @param index
		 *            the index id
		 * @param messageId
		 *            the message id
		 * @param source
		 *            the source address
		 * @param destination
		 *            the destination address
		 */
		public IndexResponseMessage(long index, UUID messageId, VodAddress source, VodAddress destination) {
			this.index = index;
			this.messageId = messageId;
            this.source = source;
            this.destination = destination;
		}

		/**
		 * Getter for the index id
		 * 
		 * @return the index id
		 */
		public long getIndex() {
			return this.index;
		}

		/**
		 * Getter for the message id
		 * 
		 * @return the message id
		 */
		public UUID getMessageId() {
			return this.messageId;
		}

        public VodAddress getSource() {
            return source;
        }

        public VodAddress getDestination() {
            return destination;
        }
    }

	/**
	 * The default event that will be routed
	 */
	public static class IndexEvent extends Event {

		/**
		 * Default constructor
		 */
		public IndexEvent() {
			super();
		}
	}

	/**
	 * An event sent to all the nodes in the leader's view whenever the index id
	 * is incremented
	 */
	public static class IndexDisseminationEvent extends IndexEvent {
		private final long index;

		/**
		 * Default constructor
		 * 
		 * @param index
		 *            the new index id
		 */
		public IndexDisseminationEvent(long index) {
			super();
			this.index = index;
		}

		/**
		 * Getter for the index id
		 * 
		 * @return the index id
		 */
		public long getIndex() {
			return index;
		}
	}

	/**
	 * Event sent from TMan to Search whenever a new leader wants to retrieve
	 * the highest index id from it's followers
	 */
	public static class StartIndexRequestEvent extends IndexEvent {
		private final UUID messageId;

		/**
		 * Default constructor
		 * 
		 * @param messageId
		 *            the message id
		 */
		public StartIndexRequestEvent(UUID messageId) {
			super();
			this.messageId = messageId;
		}

		/**
		 * Getter for the message id
		 * 
		 * @return the message id
		 */
		public UUID getMessageID() {
			return this.messageId;
		}
	}

	/**
	 * Event sent from the leader to it's followers, requesting their index id
	 */
	public static class IndexRequestEvent extends IndexEvent {
		private final long index;
		private final UUID messageId;
		private final VodAddress leaderAddress;

		/**
		 * Default constructor
		 * 
		 * @param index
		 *            the leader's current index id
		 * @param messageId
		 *            the message id
		 * @param leaderAddress
		 *            the leader's address
		 */
		public IndexRequestEvent(long index, UUID messageId, VodAddress leaderAddress) {
			super();
			this.index = index;
			this.leaderAddress = leaderAddress;
			this.messageId = messageId;
		}

		/**
		 * Getter for the index id
		 * 
		 * @return the index id
		 */
		public long getIndex() {
			return index;
		}

		/**
		 * Getter for the message id
		 * 
		 * @return the message id
		 */
		public UUID getMessageId() {
			return this.messageId;
		}

		/**
		 * Getter for the leader's address
		 * 
		 * @return the leader's address
		 */
		public VodAddress getLeaderAddress() {
			return leaderAddress;
		}
	}
}
