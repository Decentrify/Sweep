package tman.system.peer.tman;

import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * Wrapper for events routed to the leader by TMan.
 */
public class RoutedMessage extends Message {
	private static final long serialVersionUID = -4911945120291806617L;
	private final Event event;

	/**
	 * @param source
	 *            the message source
	 * @param event
	 *            the event to be routed to the leader
	 */
	protected RoutedMessage(Address source, Event event) {
		super(source, null);
		this.event = event;
	}

	/**
	 * @param source
	 *            the message source
	 * @param destination
	 *            the message destination
	 * @param event
	 *            the event to be routed to the leader
	 */
	protected RoutedMessage(Address source, Address destination, Event event) {
		super(source, destination);
		this.event = event;
	}

	/**
	 * @return the event included in this message
	 */
	public Event getEvent() {
		return event;
	}
}
