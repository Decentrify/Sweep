package se.sics.ms.simulator;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.ms.simulation.*;

public class SimulatorPort extends PortType {
	{
		positive(PeerJoin.class);
		positive(PeerFail.class);
		positive(AddIndexEntry.class);
		positive(AddMagnetEntry.class);
        positive(Search.class);
		negative(TerminateExperiment.class);
	}
}
