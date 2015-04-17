package se.sics.ms.aggregator.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.kompics.network.Transport;
import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.aggregator.type.ComponentUpdateEvent;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.p2ptoolbox.aggregator.api.msg.AggregatedStateContainer;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

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
    
    private Map<Class, Map<Integer, ComponentUpdate>> componentDataMap;
    private Logger logger = LoggerFactory.getLogger(StatusAggregator.class);
    private Negative<StatusAggregatorPort> statusAggregatorPort = provides(StatusAggregatorPort.class);
    private Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    private Positive<Timer> timerPositive = requires(Timer.class);
    
    private DecoratedAddress self;
    private DecoratedAddress globalAggregatorAddress;
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
        globalAggregatorAddress = init.getMainSimAddress();
        componentDataMap = new HashMap<Class, Map<Integer, ComponentUpdate>>();
        
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
            
            if(event.getComponentUpdate() != null){
                
                Class updateClassType = event.getComponentUpdate().getClass();
                
                if(componentDataMap.get(updateClassType)== null){
                      componentDataMap.put(updateClassType, new HashMap<Integer, ComponentUpdate>());
                }
                
                Map<Integer, ComponentUpdate> keyValue = componentDataMap.get(updateClassType);
                keyValue.put(event.getComponentUpdate().getComponentOverlay(), event.getComponentUpdate());
                
                componentDataMap.put(updateClassType, keyValue);
            }
            else{
                logger.warn("Unrecognized Component Update Received");
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

            SweepAggregatedPacket sap = new SweepAggregatedPacket(componentDataMap);
            AggregatedStateContainer container = new AggregatedStateContainer(UUID.randomUUID(),self, sap);

            logger.debug(" Trying to trigger update to , {}", globalAggregatorAddress);
            logger.debug(" Sending the below information, {}", sap);

            DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>(self, globalAggregatorAddress, Transport.UDP);
            BasicContentMsg<DecoratedAddress, DecoratedHeader, AggregatedStateContainer> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader, AggregatedStateContainer>(header, container);
            trigger(msg, networkPositive);

        }
    };
    
}
