package se.sics.ms.simulator;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.ms.scenarios.AddIndexEntry;
import se.sics.ms.scenarios.AddMagnetEntry;
import se.sics.ms.scenarios.PeerFail;
import se.sics.ms.scenarios.PeerJoin;

public class SimulatorPort extends PortType {
	{
		positive(PeerJoin.class);
		positive(PeerFail.class);
		positive(AddIndexEntry.class);
		positive(AddMagnetEntry.class);
		negative(TerminateExperiment.class);
	}
}
