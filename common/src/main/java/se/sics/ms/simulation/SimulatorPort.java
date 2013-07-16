package se.sics.ms.simulation;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class SimulatorPort extends PortType {
	{
		positive(PeerJoin.class);
		positive(PeerFail.class);
		positive(AddIndexEntry.class);
		positive(AddMagnetEntry.class);
		negative(TerminateExperiment.class);
	}
}
