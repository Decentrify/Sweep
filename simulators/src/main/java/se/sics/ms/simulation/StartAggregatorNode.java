
package se.sics.ms.simulation;

import se.sics.kompics.Init;
import se.sics.ms.main.AggregatorHostComp;
import se.sics.ms.main.AggregatorHostCompInit;
import se.sics.ms.main.TerminateConditionWrapper;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartAggregatorCmd;
import se.sics.p2ptoolbox.simulator.cmd.util.ConnectSimulatorPort;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Simulator Command to start the aggregator node in the simulation.
 *
 * Created by babbar on 2015-09-18.
 */

public class StartAggregatorNode implements StartAggregatorCmd<AggregatorHostComp, DecoratedAddress>, ConnectSimulatorPort {

    private TerminateConditionWrapper terminateConditionWrapper;
    public long timeout;
    public String fileLocation;

    public StartAggregatorNode(long timeout, String fileLocation, TerminateConditionWrapper terminateConditionWrapper){

        this.timeout = timeout;
        this.fileLocation = fileLocation;
        this.terminateConditionWrapper = terminateConditionWrapper;
    }

    public Class<AggregatorHostComp> getNodeComponentDefinition() {
        return AggregatorHostComp.class;
    }

    public Init<AggregatorHostComp> getNodeComponentInit() {
        return new AggregatorHostCompInit(timeout, fileLocation, terminateConditionWrapper);
    }

    public DecoratedAddress getAddress() {
        return SweepOperationsHelper.getAggregatorAddress();
    }
}
