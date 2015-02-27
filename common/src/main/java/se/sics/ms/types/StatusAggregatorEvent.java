package se.sics.ms.types;

import se.sics.ms.data.ComponentUpdate;
import se.sics.ms.data.GradientComponentUpdate;
import se.sics.ms.data.SearchComponentUpdate;

/**
 * 
 * The messages are supposed to be periodic updates from the components.
 * As messages arrive in order, therefore there is no need for them to contain  
 * a unique identifier as the last update win.
 *
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorEvent {


    /**
     * Status Update Event from Search Component.
     */
    public static class SearchUpdateEvent implements ComponentUpdateEvent {
        
        private final SearchComponentUpdate searchComponentUpdate;
        
        public SearchUpdateEvent(SearchComponentUpdate searchComponentUpdate){
            this.searchComponentUpdate = searchComponentUpdate;
        }

        @Override
        public ComponentUpdate getComponentUpdate() {
            return searchComponentUpdate;
        }
    }

    /**
     * Status Update Event from the Gradient Component.
     */
    public static class GradientUpdateEvent implements ComponentUpdateEvent {
        
        private final GradientComponentUpdate gradientComponentUpdate;
        
        public GradientUpdateEvent(GradientComponentUpdate gradientComponentUpdate){
            this.gradientComponentUpdate = gradientComponentUpdate;
        }

        @Override
        public ComponentUpdate getComponentUpdate() {
            return gradientComponentUpdate;
        }
    }


    /**
     * Status Update from the Leader Component.
     */
    public static class LeaderUpdateEvent implements ComponentUpdateEvent {
        
        public LeaderUpdateEvent(){
        }
        
        @Override
        public ComponentUpdate getComponentUpdate() {
            return null;
        }
    }
    
}
