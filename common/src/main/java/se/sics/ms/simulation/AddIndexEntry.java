package se.sics.ms.simulation;

import java.io.Serializable;

import se.sics.kompics.Event;

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