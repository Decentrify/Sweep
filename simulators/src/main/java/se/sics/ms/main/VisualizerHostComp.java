package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.helper.DataDump;
import se.sics.ms.helper.DataDumpInit;

/**
 * Host Component for the visualizer to be used in the system.
 *
 * Created by babbar on 2015-09-20.
 */
public class VisualizerHostComp extends ComponentDefinition {

    private static Logger logger = LoggerFactory.getLogger(VisualizerHostComp.class);

    public VisualizerHostComp(){

        logger.debug("Component initialized.");
        SimulationSerializerSetup.registerSerializers(MsConfig.SIM_SERIALIZER_START);
        subscribe(startHandler, control);
    }



    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {

            logger.debug("Component started.");

            Component dataDumpRead = create(DataDump.Read.class, new DataDumpInit.Read(MsConfig.SIMULATION_FILE_LOC));
            trigger(Start.event, dataDumpRead.control());
            subscribe(aggregatedInfoHandler, dataDumpRead.getPositive(GlobalAggregatorPort.class));

        }
    };


    Handler<AggregatedInfo> aggregatedInfoHandler = new Handler<AggregatedInfo>() {
        @Override
        public void handle(AggregatedInfo aggregatedInfo) {
            logger.debug("Handling the aggregated info packet from the data read component.");
            logger.debug("{}:", aggregatedInfo.getNodePacketMap());
        }
    };

    public static void main(String[] args) {

        logger.debug("Launching the component.");
        if (Kompics.isOn()) {
            Kompics.shutdown();
        }
        Kompics.createAndStart(VisualizerHostComp.class, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            System.exit(1);
        }

    }

}
