package se.sics.ms.simulation;

import se.sics.kompics.Event;

import java.io.Serializable;

public final class PeerFail extends Event implements Serializable {
	private static final long serialVersionUID = 719727908389357757L;
	private final Long id;

	public PeerFail(Long id) {
		this.id = id;
	}

	public Long getId() {
		return id;
	}
}
