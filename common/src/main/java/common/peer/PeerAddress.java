package common.peer;

import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.overlay.OverlayAddress;

/**
 * Wrapper for the Address used to send it to the bootstrapping server
 */
public class PeerAddress extends OverlayAddress {
	private static final long serialVersionUID = 5602304921536972253L;

	public PeerAddress(Address peerAddress) {
		super(peerAddress);
	}
}
