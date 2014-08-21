package se.sics.ms.simulation;

import se.sics.kompics.Event;

import java.io.Serializable;

public final class PeerJoin extends Event implements Serializable {
	private static final long serialVersionUID = -5561956271621781131L;
	private final Long peerId;

	public PeerJoin(Long peerId) {
		this.peerId = peerId;
	}

	public Long getPeerId() {
		return this.peerId;
	}
}
