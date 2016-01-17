package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.aggregator.AggregatorSerializerSetup;
import se.sics.ktoolbox.aggregator.server.GlobalAggregator;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorInit;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.helper.*;
import se.sics.ms.net.SweepSerializerSetup;
import se.sics.p2ptoolbox.simulator.ExperimentPort;
import se.sics.p2ptoolbox.simulator.dsl.events.TerminateExperiment;

/**
 * Main host component for the aggregator in the system.
 *
 * Created by babbar on 2015-09-18.
 */
public class AggregatorHostComp extends ComponentDefinition{

    private Logger logger = LoggerFactory.getLogger(AggregatorHostComp.class);
    Positive<Network> network = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<ExperimentPort> experimentPort = requires(ExperimentPort.class);

    private long timeout;
    private String fileLocation;
    private int maxNodes;
    private FinalStateInfo finalState;
    private FinalStateProcessor stateProcessor;

    public AggregatorHostComp(AggregatorHostCompInit init){

        logger.debug("Booted the aggregator host component.");

        doInit(init);
        subscribe(startHandler, control);
        subscribe(experimentTermination, experimentPort);
    }

    public void doInit(AggregatorHostCompInit init){


        this.timeout = init.timeout;
        this.fileLocation = init.fileLocation;
        this.maxNodes = init.conditionWrapper.numNodes;
        this.finalState = init.conditionWrapper.finalState;
        this.stateProcessor = init.conditionWrapper.stateProcessor;

        int result = SweepSerializerSetup.registerSerializers(MsConfig.SIM_SERIALIZER_START);
        AggregatorSerializerSetup.registerSerializers(result);
        DataDump.register(MsConfig.SIMULATION_DIRECTORY, MsConfig.SIMULATION_FILENAME);
    }

    /**
     * Main start handler for the component.
     * Create the child components and form the connections.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {

            logger.debug("Handling the start event in the system.");

            Component globalAggregator = create(GlobalAggregator.class, new GlobalAggregatorInit(timeout));
            Component dataDumpWrite = create(DataDump.Write.class, new DataDumpInit.Write(new BasicHelper(), MsConfig.MAX_WINDOWS_PER_FILE));
            Component stateTermination = create(SimulationTermination.class, new SimulationTermination.SimulationTerminationInit(maxNodes, finalState, stateProcessor));

            connect(dataDumpWrite.getNegative(GlobalAggregatorPort.class), globalAggregator.getPositive(GlobalAggregatorPort.class));
            connect(dataDumpWrite.getNegative(ExperimentPort.class), experimentPort);

            connect(stateTermination.getNegative(GlobalAggregatorPort.class), globalAggregator.getPositive(GlobalAggregatorPort.class));
            connect(stateTermination.getNegative(ExperimentPort.class), experimentPort);

            connect(globalAggregator.getNegative(Network.class), network);
            connect(globalAggregator.getNegative(Timer.class), timer);

            logger.debug("Creating the data dump component.");

            trigger(Start.event, globalAggregator.control());
            trigger(Start.event, dataDumpWrite.control());
            trigger(Start.event, stateTermination.control());
        }
    };



    Handler<TerminateExperiment> experimentTermination = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment terminateExperiment) {
            logger.debug("Going to terminate the experiment.");
        }
    };

}
