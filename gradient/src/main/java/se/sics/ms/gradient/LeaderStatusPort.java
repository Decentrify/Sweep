package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;

public class LeaderStatusPort extends PortType {
	{
		negative(LeaderStatus.class);
		negative(NodeCrashEvent.class);
        positive(TerminateBeingLeader.class);
	}

	/**
	 * QueryLimit message used to broadcast the leader's status
	 */
	public static class LeaderStatus extends Event {
		private final boolean isLeader;

		/**
		 * Default constructor
		 * 
		 * @param isLeader
		 *            true if the node is the leader
		 */
		public LeaderStatus(boolean isLeader) {
			super();

			this.isLeader = isLeader;
		}

		/**
		 * Getter for the leader status
		 * 
		 * @return true if the node is the leader
		 */
		public boolean isLeader() {
			return this.isLeader;
		}
	}

	/**
	 * An event sent whenever the a node such as the leader has been established
	 * as dead
	 */
	public static class NodeCrashEvent extends Event {
		private final VodAddress deadNode;

		/**
		 * Default constructor
		 * 
		 * @param deadNode
		 *            the address of the dead node
		 */
		public NodeCrashEvent(VodAddress deadNode) {
			super();
			this.deadNode = deadNode;
		}

		/**
		 * Getter for the address of the dead node
		 * 
		 * @return the address of the dead node
		 */
		public VodAddress getDeadNode() {
			return deadNode;
		}
	}

    public static class TerminateBeingLeader extends Event {
        public TerminateBeingLeader() {
            super();
        }
    }
}
