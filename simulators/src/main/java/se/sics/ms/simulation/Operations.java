package se.sics.ms.simulation;

import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;


@SuppressWarnings("serial")
public class Operations {
	// Global counter to create monotonically increasing ids
	private static long nextId = 0;

	public static Operation1<AddIndexEntry, Long> addIndexEntry() {
		return new Operation1<AddIndexEntry, Long>() {
			@Override
			public AddIndexEntry generate(Long id) {
				return new AddIndexEntry(id);
			}
		};
	}

	/**
	 * Create a {@link PeerJoin} event with a new monotonically increasing id.
	 * 
	 * @return a new {@link PeerJoin} event
	 */
	public static Operation<PeerJoin> peerJoin() {
		return new Operation<PeerJoin>() {
			@Override
			public PeerJoin generate() {
				return new PeerJoin(nextId++);
			}
		};
	}

	public static Operation1<PeerFail, Long> peerFail() {
		return new Operation1<PeerFail, Long>() {
			@Override
			public PeerFail generate(Long id) {
				return new PeerFail(id);
			}
		};
	}

	public static Operation<Publish> publish() {
		return new Operation<Publish>() {
			@Override
			public Publish generate() {
				return new Publish();
			}
		};
	}

	/**
	 * Create an event to query the simulator to add an magnetic link entry from
	 * a given xml file.
	 * 
	 * @return a new {@link AddMagnetEntry} event
	 */
	public static Operation1<AddMagnetEntry, Long> addMagnetEntry() {
		return new Operation1<AddMagnetEntry, Long>() {
			@Override
			public AddMagnetEntry generate(Long id) {
				return new AddMagnetEntry(id);
			}
		};
	}
}
