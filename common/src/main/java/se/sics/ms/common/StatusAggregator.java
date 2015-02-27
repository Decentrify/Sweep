package se.sics.ms.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.ms.data.ComponentUpdate;
import se.sics.ms.data.GradientComponentUpdate;
import se.sics.ms.data.SearchComponentUpdate;
import se.sics.ms.ports.StatusAggregatorPort;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.AggregatorUpdateMsg;
import se.sics.ms.types.ComponentUpdateEvent;
import se.sics.ms.types.StatusAggregatorEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Status Aggregator Component responsible for collecting status of the components inside the peer.
 * The status collected is periodically pushed over the network to the listening components.
 *  
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregator extends ComponentDefinition{
    
    private Map<String, ComponentUpdate> componentDataMap;
    private Logger logger = LoggerFactory.getLogger(StatusAggregator.class);
    private Negative<StatusAggregatorPort> statusAggregatorPort = provides(StatusAggregatorPort.class);
    private Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    private Positive<Timer> timerPositive = requires(Timer.class);
    
    private VodAddress self;
    private VodAddress simComponentAddress;
    private int timeout_seconds;
    
    
    public StatusAggregator(StatusAggregatorInit init){
        doInit(init);
        subscribe(startHandler,control);
        subscribe(componentStatusUpdateHandler, statusAggregatorPort);
        subscribe(periodicStateUpdateDispenseEvent, timerPositive);
    }
    
    
    private class AggregateStateUpdateTimeout extends IndividualTimeout{

        public AggregateStateUpdateTimeout(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }
    
    private void doInit(StatusAggregatorInit init){
        self = init.getSelf();
        timeout_seconds = init.getTimeout();
        simComponentAddress = init.getMainSimAddress();
        componentDataMap = new HashMap<String, ComponentUpdate>();
    }
    
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            
            logger.info("Aggregator: Started.");
            
            if(simComponentAddress != null){  
                // == Schedule Periodic Timeout, only if there is a component, listening updates.
                logger.info("Aggregator: Triggering the timeout.");
                SchedulePeriodicTimeout spt;
                spt = new SchedulePeriodicTimeout(timeout_seconds, timeout_seconds);
                spt.setTimeoutEvent(new AggregateStateUpdateTimeout(spt, self.getId()));
                trigger(spt, timerPositive);
            }
            
        }
    };

    
    /**
     * Status Update from different components are handled by this handler.
     * Simply push the update to the map.
     */
    Handler<ComponentUpdateEvent> componentStatusUpdateHandler = new Handler<ComponentUpdateEvent>() {
        @Override
        public void handle(ComponentUpdateEvent event) {
            
            String mapKey = null;
            
            if(event instanceof StatusAggregatorEvent.SearchUpdateEvent){
                mapKey = ComponentUpdateEnum.SEARCH.getName();
            }
            
            else if(event instanceof StatusAggregatorEvent.GradientUpdateEvent){
                mapKey = ComponentUpdateEnum.GRADIENT.getName();
            }
            else{
                logger.warn("Status update from unknown component.");
            }
            
            if(mapKey !=null){
                logger.info("Adding data to component map with key {}", mapKey);
                componentDataMap.put(mapKey, event.getComponentUpdate());
            }
        }
    };

    /**
     * Periodic Timeout Handler.
     * Push the state collected over the network.
     */
    Handler<AggregateStateUpdateTimeout> periodicStateUpdateDispenseEvent = new Handler<AggregateStateUpdateTimeout>(){
        @Override
        public void handle(AggregateStateUpdateTimeout event) {
            logger.info("Aggregator: Pushing periodic data update.");
            trigger(new AggregatorUpdateMsg(self, simComponentAddress,componentDataMap), networkPositive);
        }
    };
    
    
    
    
    
}
