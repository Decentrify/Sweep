package common.simulation;

import java.io.Serializable;

import se.sics.kompics.Event;

/**
 * Event used to tell the simulator to add an index entry from the file storing
 * magnetic links.
 */
public final class AddMagnetEntry extends Event implements Serializable {
	private static final long serialVersionUID = -3450667757500591816L;
	private final Long id;

	/**
	 * @param id
	 *            the id of the node which should execute the adding request
	 */
	public AddMagnetEntry(Long id) {
		this.id = id;
	}

	/**
	 * @return the id of the node which should execute the adding request
	 */
	public Long getId() {
		return id;
	}
}
