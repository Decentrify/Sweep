
package se.sics.ms.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.p2ptoolbox.simulator.ExperimentPort;
import se.sics.p2ptoolbox.simulator.dsl.events.TerminateExperiment;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Main component class used to determine when the
 * simulation needs to be terminated.
 *
 * Created by babbar on 2015-09-18.
 */
public class SimulationTermination <FS extends FinalStateInfo> extends ComponentDefinition {

    private Logger logger = LoggerFactory.getLogger(SimulationTermination.class);
    private FS finalStateInfo;
    private FinalStateProcessor<PacketInfo, FS> finalStateProcessor;

    Positive<ExperimentPort> experimentPort = requires(ExperimentPort.class);

    Positive<GlobalAggregatorPort> globalAggregatorPort = requires(GlobalAggregatorPort.class);


    public SimulationTermination(SimulationTerminationInit<PacketInfo, FS> init){

        logger.debug("Component initialized");

        finalStateInfo = init.finalState;
        finalStateProcessor = init.processor;

        subscribe(startHandler, control);
        subscribe(aggregatedInfoHandler, globalAggregatorPort);
    }


    /**
     * Handler for the start event in the system.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {
            logger.debug("Handling the start event.");
        }
    };


    /**
     * Handler for the information received from the global aggregator in the
     * system.
     *
     */
    Handler<AggregatedInfo> aggregatedInfoHandler = new Handler<AggregatedInfo>() {
        @Override
        public void handle(AggregatedInfo aggregatedInfo) {

            logger.debug("Received the information from the global aggregator.");

            Map<Integer, List<PacketInfo>> nodePacketMap = aggregatedInfo.getNodePacketMap();
            Collection<FS> finalStateObjects = finalStateProcessor.process(nodePacketMap);

            boolean result = true;
            for(FS state : finalStateObjects){

                result = (state.equals(finalStateInfo));
                if(!result)
                    break;
            }

            if(result){
                logger.debug("Time to terminate the experiment");
                trigger(new TerminateExperiment(), experimentPort);
            }
        }
    };




    public static class SimulationTerminationInit<PI_I extends PacketInfo, FS extends FinalStateInfo> extends Init<SimulationTermination<FS>>{

        public FS finalState;
        public FinalStateProcessor<PI_I, FS> processor;


        public SimulationTerminationInit(FS finalState, FinalStateProcessor<PI_I, FS> finalStateProcessor){
            this.finalState = finalState;
            this.processor  = finalStateProcessor;
        }


    }




}
