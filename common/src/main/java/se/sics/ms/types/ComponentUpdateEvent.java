package se.sics.ms.types;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.data.ComponentUpdate;

/**
 * Created by babbarshaer on 2015-02-19.
 */
public interface ComponentUpdateEvent extends KompicsEvent{

    /**
     * Get the component status.
     * @return Component Status Data.
     */
    public ComponentUpdate getComponentUpdate();
}
