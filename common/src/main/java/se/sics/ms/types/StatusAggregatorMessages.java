package se.sics.ms.types;

import se.sics.ms.data.ComponentStatus;
import se.sics.ms.data.GradientStatusData;
import se.sics.ms.data.SearchStatusData;

/**
 * 
 * The messages are supposed to be periodic updates from the components.
 * As messages arrive in order, therefore there is no need for them to contain  
 * a unique identifier as the last update win.
 *
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorMessages {


    /**
     * Status Update Event from Search Component.
     */
    public static class SearchUpdateEvent implements ComponentUpdateEvent {
        
        private final SearchStatusData searchStatusData;
        
        public SearchUpdateEvent(SearchStatusData searchStatusData){
            this.searchStatusData = searchStatusData;
        }

        @Override
        public ComponentStatus getComponentStatus() {
            return searchStatusData;
        }
    }

    /**
     * Status Update Event from the Gradient Component.
     */
    public static class GradientUpdateEvent implements ComponentUpdateEvent {
        
        private final GradientStatusData gradientStatusData;
        
        public GradientUpdateEvent(GradientStatusData gradientStatusData){
            this.gradientStatusData = gradientStatusData;
        }

        @Override
        public ComponentStatus getComponentStatus() {
            return gradientStatusData;
        }
    }


    /**
     * Status Update from the Leader Component.
     */
    public static class LeaderUpdateEvent implements ComponentUpdateEvent {
        
        public LeaderUpdateEvent(){
        }
        
        @Override
        public ComponentStatus getComponentStatus() {
            return null;
        }
    }
    
}
