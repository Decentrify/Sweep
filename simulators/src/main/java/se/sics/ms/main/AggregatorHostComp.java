package se.sics.ms.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.aggregator.server.GlobalAggregator;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorInit;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ms.helper.DataDump;
import se.sics.ms.helper.DataDumpInit;
import se.sics.p2ptoolbox.simulator.ExperimentPort;

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

    public AggregatorHostComp(AggregatorHostCompInit init){

        logger.debug("Booted the aggregator host component.");

        doInit(init);
        subscribe(startHandler, control);
    }

    public void doInit(AggregatorHostCompInit init){

        this.timeout = init.timeout;
        this.fileLocation = init.fileLocation;
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
            Component dataDumpWrite = create(DataDump.Write.class, new DataDumpInit.Write(fileLocation));

            connect(dataDumpWrite.getNegative(GlobalAggregatorPort.class), globalAggregator.getPositive(GlobalAggregatorPort.class));
            connect(globalAggregator.getNegative(Network.class), network);
            connect(globalAggregator.getNegative(Timer.class), timer);

            logger.debug("Creating the data dump component.");
            trigger(Start.event, globalAggregator.control());
            trigger(Start.event, dataDumpWrite.control());

            System.exit(-1);
        }
    };

}
