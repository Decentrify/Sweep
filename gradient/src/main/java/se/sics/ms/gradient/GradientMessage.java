package se.sics.ms.gradient;

import java.util.Collection;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.Event;

/**
 * Superclass used for TMan components to exchange identifiers with each other.
 */
public abstract class GradientMessage extends Event {
	private static final long serialVersionUID = 8493601671018888143L;
	private final UUID requestId;
	private final Collection<VodAddress> exchangeCollection;
    private final VodAddress source;
    private final VodAddress destination;

	/**
	 * @param requestId
	 *            the unique request id
	 * @param exchangeCollection
	 *            a collection of nodes for exchange
	 * @param source
	 *            the message source
	 * @param destination
	 *            the message destination
	 */
	public GradientMessage(UUID requestId, Collection<VodAddress> exchangeCollection, VodAddress source,
			VodAddress destination) {
        super();
		this.requestId = requestId;
		this.exchangeCollection = exchangeCollection;
        this.source = source;
        this.destination = destination;
	}

	/**
	 * @return the unique request id
	 */
	public UUID getRequestId() {
		return requestId;
	}

    public VodAddress getSource() {
        return source;
    }

    public VodAddress getDestination() {
        return destination;
    }

    /**
	 * @return a collection of nodes for exchange
	 */
	public Collection<VodAddress> getExchangeCollection() {
		return exchangeCollection;
	}

	/**
	 * A request to exchange addresses.
	 */
	public static class TManRequest extends GradientMessage {
		private static final long serialVersionUID = -8763776600634619932L;

		/**
		 * @param requestId
		 *            the unique request id
		 * @param exchangeCollection
		 *            a collection of nodes for exchange
		 * @param source
		 *            the message source
		 * @param destination
		 *            the message destination
		 */
		public TManRequest(UUID requestId, Collection<VodAddress> exchangeCollection, VodAddress source,
				VodAddress destination) {
			super(requestId, exchangeCollection, source, destination);
		}
	}

	/**
	 * A response to a {@link TManRequest}.
	 */
	public static class TManResponse extends GradientMessage {
		private static final long serialVersionUID = -8094193425301016275L;

		/**
		 * @param requestId
		 *            the unique request id
		 * @param exchangeCollection
		 *            a collection of nodes for exchange
		 * @param source
		 *            the message source
		 * @param destination
		 *            the message destination
		 */
		public TManResponse(UUID requestId, Collection<VodAddress> exchangeCollection, VodAddress source,
				VodAddress destination) {
			super(requestId, exchangeCollection, source, destination);
		}
	}
}