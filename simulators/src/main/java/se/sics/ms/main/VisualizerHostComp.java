package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.aggregator.AggregatorSerializerSetup;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ktoolbox.aggregator.server.Visualizer;
import se.sics.ktoolbox.aggregator.server.VisualizerInit;
import se.sics.ktoolbox.aggregator.server.VisualizerPort;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.server.event.WindowProcessing;
import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
import se.sics.ms.aggregator.design.PercentileLagDesignInfo;
import se.sics.ms.aggregator.design.PercentileLagDesignInfoContainer;
import se.sics.ms.aggregator.design.ReplicationLagDesignInfo;
import se.sics.ms.aggregator.design.ReplicationLagDesignInfoContainer;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.helper.*;
import se.sics.ms.net.SweepSerializerSetup;
import sun.awt.X11.Visual;

import java.io.IOException;
import java.util.*;

/**
 * Host Component for the visualizer to be used in the system.
 *
 * Created by babbar on 2015-09-20.
 */
public class VisualizerHostComp extends ComponentDefinition {

    private static Logger logger = LoggerFactory.getLogger(VisualizerHostComp.class);
    private Component timer;
    private Component visualizer;

    public VisualizerHostComp(){

        logger.debug("Component initialized.");

        int result = SweepSerializerSetup.registerSerializers(MsConfig.SIM_SERIALIZER_START);
        AggregatorSerializerSetup.registerSerializers(result);

        DataDump.register(MsConfig.SIMULATION_DIRECTORY, MsConfig.SIMULATION_FILENAME);
        subscribe(startHandler, control);
    }



    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {

            logger.debug("Component started.");

            timer = create(JavaTimer.class, Init.NONE);

            Component dataDumpRead = create(DataDump.Read.class, new DataDumpInit.Read(5000));
            connect(dataDumpRead.getNegative(Timer.class), timer.getPositive(Timer.class));

            visualizer = create(Visualizer.class, new VisualizerInit(Integer.MAX_VALUE, getDesignProcessorMap()));
            connect(visualizer.getNegative(GlobalAggregatorPort.class), dataDumpRead.getPositive(GlobalAggregatorPort.class));

            trigger(Start.event, timer.control());
            trigger(Start.event, visualizer.control());
            trigger(Start.event, dataDumpRead.control());

            ScheduleTimeout st = new ScheduleTimeout(5000);
            st.setTimeoutEvent(new ResultTimeout(st));

//            trigger(st, timer.getPositive(Timer.class));

            subscribe(resultTimeoutHandler, timer.getPositive(Timer.class));
            subscribe(replicationLagResponse, visualizer.getPositive(VisualizerPort.class));
            subscribe(percentileReplicationLag, visualizer.getPositive(VisualizerPort.class));
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


    private Map<String, DesignProcessor> getDesignProcessorMap(){

        Map<String, DesignProcessor> result = new HashMap<String, DesignProcessor>();
        for(SimDesignerEnum val : SimDesignerEnum.values()){
            result.put(val.getName(), val.getProcessor());
        }

        return result;
    }

    /**
     * Timeout event occured indicating the triggering
     * of the event to executing the processor over the aggregated data and extract
     * information to be rendered to the UI.
     */
    Handler<ResultTimeout> resultTimeoutHandler = new Handler<ResultTimeout>() {
        @Override
        public void handle(ResultTimeout resultTimeout) {

            logger.debug("Time to request for a processed result from the visualizer info.");
            WindowProcessing.Request request = new WindowProcessing.Request(UUID.randomUUID(),
                    SimDesignerEnum.ReplicationLagDesigner.getName(),
                    0, Integer.MAX_VALUE);

            WindowProcessing.Request percentileLagRequest = new WindowProcessing.Request(UUID.randomUUID(),
                    SimDesignerEnum.PercentileLagDesigner.getName(),
                    0 , Integer.MAX_VALUE);

            trigger(request, visualizer.getPositive(VisualizerPort.class));
            trigger(percentileLagRequest, visualizer.getPositive(VisualizerPort.class));
        }
    };


    ClassMatchedHandler<ReplicationLagDesignInfoContainer, WindowProcessing.Response<ReplicationLagDesignInfoContainer>> replicationLagResponse = new ClassMatchedHandler<ReplicationLagDesignInfoContainer, WindowProcessing.Response<ReplicationLagDesignInfoContainer>>() {
        @Override
        public void handle(ReplicationLagDesignInfoContainer replicationLagDesignInfoContainer, WindowProcessing.Response<ReplicationLagDesignInfoContainer> content) {

            logger.debug("Received response from the visualizer component about the average lag information.");
            Collection<ReplicationLagDesignInfo> result = content.getContent().getProcessedWindows();
            List<ReplicationLagDesignInfo> reversedList = new ArrayList<ReplicationLagDesignInfo>(result);
            Collections.reverse(reversedList);

            try {
                logger.debug("Creating a JSON Dump File.");
                performJSONDump(reversedList, MsConfig.AVG_LAG_JSON_DUMP_FILE);
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to create the JSON Dump File.");
            }
        }
    };



    ClassMatchedHandler<PercentileLagDesignInfoContainer, WindowProcessing.Response<PercentileLagDesignInfoContainer>> percentileReplicationLag = new ClassMatchedHandler<PercentileLagDesignInfoContainer, WindowProcessing.Response<PercentileLagDesignInfoContainer>>() {
        @Override
        public void handle(PercentileLagDesignInfoContainer replicationLagDesignInfoContainer, WindowProcessing.Response<PercentileLagDesignInfoContainer> content) {

            logger.debug("Received response from the visualizer component about the percentile lag information.");

            Collection<PercentileLagDesignInfo> result = content.getContent().getProcessedWindows();
            List<PercentileLagDesignInfo> reversedList = new ArrayList<PercentileLagDesignInfo>(result);
            Collections.reverse(reversedList);

            try {
                logger.debug("Creating a JSON Dump File.");
                performPercentileLagJSONDump(reversedList, MsConfig.PER_LAG_JSON_DUMP_FILE);
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to create the JSON Dump File.");
            }
        }
    };



    private void performPercentileLagJSONDump(List<PercentileLagDesignInfo> list, String location) throws IOException {
        JSONDump.dumpPercentileLagInfo(list, location);
    }


    private void performJSONDump(List<ReplicationLagDesignInfo>list,  String fileLocation) throws IOException {
        JSONDump.dumpSystemLagInfo(list, fileLocation);
    }

    private class ResultTimeout extends Timeout{

        public ResultTimeout(ScheduleTimeout request) {
            super(request);
        }

    }
}
