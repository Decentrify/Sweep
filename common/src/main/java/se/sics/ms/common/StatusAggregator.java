package se.sics.ms.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.ms.data.ComponentUpdate;
import se.sics.ms.data.GradientComponentUpdate;
import se.sics.ms.data.SearchComponentUpdate;
import se.sics.ms.ports.StatusAggregatorPort;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.*;
import se.sics.p2ptoolbox.aggregator.api.msg.AggregatedStateContainer;
import se.sics.p2ptoolbox.aggregator.core.msg.AggregatorNetMsg;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
    private VodAddress globalAggregatorAddress;
    private int timeout_seconds;
    
    
    public StatusAggregator(StatusAggregatorInit init){
        doInit(init);
        subscribe(startHandler,control);
        subscribe(componentStatusUpdateHandler, statusAggregatorPort);
        subscribe(periodicStateUpdateDispenseEvent, timerPositive);
        subscribe(oneTimeUpdateHandler, timerPositive);
    }
    
    
    private class AggregateStateUpdateTimeout extends IndividualTimeout{

        public AggregateStateUpdateTimeout(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }
    
    // Only for testing.
    private class OneTimeUpdate extends IndividualTimeout{
        
        public OneTimeUpdate(ScheduleTimeout request , int id){
            super(request, id);
        }
    }
    
    
    private void doInit(StatusAggregatorInit init){
        self = init.getSelf();
        timeout_seconds = init.getTimeout();
        globalAggregatorAddress = init.getMainSimAddress();
        componentDataMap = new HashMap<String, ComponentUpdate>();
    }
    
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            
            logger.info("Local Aggregator: Started.");
            
            if(globalAggregatorAddress != null){

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
     * Simply push the update to the map for now.
     *
     * Also in order to keep things simple, the component must send a copy of the update in order to prevent references floating around.
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
            else{
                logger.warn("Received Update from unrecognizable component.");
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

            logger.info("Sending Periodic Update to Global Aggregator Component");

            SweepAggregatedPacket sap = createCondensedStatusUpdate(componentDataMap);
            AggregatedStateContainer container = new AggregatedStateContainer(self, sap);

            logger.warn(" Trying to trigger update to , {}", globalAggregatorAddress);
            logger.warn(" Sending the below information, {}", sap);

            trigger(new AggregatorNetMsg.OneWay(self, globalAggregatorAddress, UUID.randomUUID(), container), networkPositive);

        }
    };

    
    Handler<OneTimeUpdate> oneTimeUpdateHandler = new Handler<OneTimeUpdate>() {
        @Override
        public void handle(OneTimeUpdate event) {
            logger.info("Aggregator: Pushing One time update to the scheduler.");
            trigger(new AggregatorUpdateMsg(self, globalAggregatorAddress,componentDataMap), networkPositive);
        }
    };


    /**
     * Based on the provided map, extract the values
     * and create a condensed packet information to be sent to the Aggregator Component.
     *
     * @param componentUpdateMap component update map.
     * @return packet containing state information.
     */
    private SweepAggregatedPacket createCondensedStatusUpdate(Map<String, ComponentUpdate> componentUpdateMap){

        SweepAggregatedPacket sap = new SweepAggregatedPacket(self.getId());

        for(Map.Entry<String, ComponentUpdate> entry : componentUpdateMap.entrySet()){

            String key = entry.getKey();
            ComponentUpdate value = entry.getValue();

            if(key.equals(ComponentUpdateEnum.SEARCH.getName()) && value instanceof SearchComponentUpdate){
                SearchComponentUpdate scup = (SearchComponentUpdate)value;
                SearchDescriptor desc  = scup.getSearchDescriptor();

                if(desc != null){
                    sap.setPartitionId(desc.getOverlayId().getPartitionId());
                    sap.setPartitionDepth(desc.getOverlayId().getPartitionIdDepth());
                    sap.setNumberOfEntries(desc.getNumberOfIndexEntries());
                }
            }
        }

        return sap;
    }
    
}
