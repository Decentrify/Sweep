package se.sics.ms.simulator;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.ms.simulation.AddIndexEntry;
import se.sics.ms.simulation.AddMagnetEntry;
import se.sics.ms.simulation.PeerFail;
import se.sics.ms.simulation.PeerJoin;

public class SimulatorPort extends PortType {
	{
		positive(PeerJoin.class);
		positive(PeerFail.class);
		positive(AddIndexEntry.class);
		positive(AddMagnetEntry.class);
		negative(TerminateExperiment.class);
	}
}
