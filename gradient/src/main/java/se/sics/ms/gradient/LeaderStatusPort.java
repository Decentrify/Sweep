package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;

/**
 * This port class is created to handle the acquiring and dissemination of the
 * leader's current status
 */
public class LeaderStatusPort extends PortType {
	{
		negative(LeaderStatus.class);
		negative(NodeCrashEvent.class);
		negative(NodeSuggestion.class);
		negative(LeaderStatusRequest.class);
		negative(LeaderStatusResponse.class);

		positive(LeaderStatusRequest.class);
		positive(LeaderStatusResponse.class);
	}

	/**
	 * A message used to broadcast the leader's status
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

	/**
	 * An event carrying a suggestion for the Gradient view
	 */
	public static class NodeSuggestion extends Event {
		private final VodAddress suggestion;

		/**
		 * Default constructor
		 * 
		 * @param suggestion
		 *            the suggested address
		 */
		public NodeSuggestion(VodAddress suggestion) {
			super();
			this.suggestion = suggestion;
		}

		/**
		 * Getter for the suggested address
		 * 
		 * @return the suggested address
		 */
		public VodAddress getSuggestion() {
			return this.suggestion;
		}
	}

	/**
	 * An event used to request the status of the node
	 */
	public static class LeaderStatusRequest extends Event {

		/**
		 * Default constructor
		 */
		public LeaderStatusRequest() {
			super();
		}
	}

	/**
	 * An event used to respond to a leader status request
	 */
	public static class LeaderStatusResponse extends Event {
		private final VodAddress leader;

		/**
		 * Default constructor
		 * 
		 * @param leader
		 *            the address of the leader
		 */
		public LeaderStatusResponse(VodAddress leader) {
			super();
			this.leader = leader;
		}

		/**
		 * Getter for the leader's address
		 * 
		 * @return the address of the leader
		 */
		public VodAddress getLeader() {
			return this.leader;
		}
	}
}
