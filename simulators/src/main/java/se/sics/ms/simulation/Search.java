package se.sics.ms.simulation;

import se.sics.kompics.Event;

import java.io.Serializable;

/**
 *
 * @author: Steffen Grohsschmiedt
 */
public final class Search extends Event implements Serializable {
    private final Long id;

    public Search(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }
}

