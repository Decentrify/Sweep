package se.sics.ms.aggregator.type;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.util.ComponentUpdateEnum;

/**
 * Marker Interface representing updates from different components.
 * Created by babbarshaer on 2015-02-19.
 */
public interface ComponentUpdateEvent extends KompicsEvent{

    /**
     * Get the component status.
     * @return Component Status Data.
     */
    public ComponentUpdate getComponentUpdate();
}
