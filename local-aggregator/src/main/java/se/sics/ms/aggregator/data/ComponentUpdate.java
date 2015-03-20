package se.sics.ms.aggregator.data;

import se.sics.ms.aggregator.util.ComponentUpdateEnum;

/**
 * Marker Interface for the data objects from different components.
 * 
 * Created by babbarshaer on 2015-02-19.
 */
public interface ComponentUpdate {

    /**
     * In order to differentiate between the different instances of same service,
     * they should send me the data with different overlay id.
     *
     * @return Component OverlayId.
     */
    public int getComponentOverlay();
    
}
