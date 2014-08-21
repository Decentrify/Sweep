package se.sics.ms.simulation;

import se.sics.kompics.Event;

import java.io.Serializable;

public final class AddIndexEntry extends Event implements Serializable {
	private static final long serialVersionUID = -8128102866273386943L;
	private final Long id;

	public AddIndexEntry(Long id) {
		this.id = id;
	}

	public Long getId() {
		return id;
	}
}
