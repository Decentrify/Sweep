package tman.system.peer.tman;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * Wrapper for events routed to the leader by TMan.
 */
public class RoutedMessage extends Event {
	private static final long serialVersionUID = -4911945120291806617L;
	private final Event event;
    private final VodAddress source;
    private VodAddress destination;

	/**
	 * @param source
	 *            the message source
	 * @param event
	 *            the event to be routed to the leader
	 */
	protected RoutedMessage(VodAddress source, Event event) {
        this.source = source;
		this.event = event;

        this.destination = null;
	}

	/**
	 * @param source
	 *            the message source
	 * @param destination
	 *            the message destination
	 * @param event
	 *            the event to be routed to the leader
	 */
	protected RoutedMessage(VodAddress source, VodAddress destination, Event event) {
        this.source = source;
        this.destination = destination;
		this.event = event;
	}

	/**
	 * @return the event included in this message
	 */
	public Event getEvent() {
		return event;
	}

    public VodAddress getSource() {
        return source;
    }

    public VodAddress getDestination() {
        return destination;
    }


    public void setDestination(VodAddress destination) {
        this.destination = destination;
    }
}
