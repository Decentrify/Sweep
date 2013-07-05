package election.system.peer.election;

import java.util.Collection;
import java.util.UUID;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * Messages that are sent back and forth to the election components
 */
public class VotingMsg {

	/**
	 * A parent message class that the other messages will inherit.
	 */
	public static class ElectionMsg extends Message {
		private static final long serialVersionUID = -1628433989583269449L;
		private final UUID requestId;

		/**
		 * Constructor for an election message
		 * 
		 * @param source
		 *            the source address
		 * @param destination
		 *            the destination address
		 */
		public ElectionMsg(Address source, Address destination) {
			super(source, destination);
			this.requestId = null;
		}

		/**
		 * Constructor for an election message
		 * 
		 * @param requestId
		 *            a UUID representing a timeout
		 * @param source
		 *            the source address
		 * @param destination
		 *            the destination address
		 */
		public ElectionMsg(UUID requestId, Address source, Address destination) {
			super(source, destination);
			this.requestId = requestId;
		}

		/**
		 * Getter for the request ID
		 * 
		 * @return the request UUID belonging to a timeout
		 */
		public UUID getRequestId() {
			return requestId;
		}
	}

	/**
	 * Message used when a leader candidate wants to vote for leadership
	 */
	public static class VotingRequestMsg extends ElectionMsg {
		private static final long serialVersionUID = 4798983776527412760L;
		private int voteID;

		/**
		 * Constructor for voting request message
		 * 
		 * @param voteID
		 *            the ID defining that batch of votes
		 * @param requestId
		 *            the timeout UUID
		 * @param source
		 *            the source address
		 * @param destination
		 *            the destination address
		 */
		public VotingRequestMsg(int voteID, UUID requestId, Address source, Address destination) {
			super(requestId, source, destination);
			this.voteID = voteID;
		}

		/**
		 * Getter for the vote ID
		 * 
		 * @return the ID of this batch's votes
		 */
		public int getVoteID() {
			return this.voteID;
		}
	}

	/**
	 * Response message from peers to the leader candidate
	 */
	public static class VotingResponseMsg extends ElectionMsg {
		private static final long serialVersionUID = 7421215345594628908L;
		private int voteID;
		private Address highestNode;
		private boolean vote, isConverged;

		/**
		 * Constructor for voting response message
		 * 
		 * @param isConverged
		 *            whether the node has converged with its neighbors or not
		 * @param voteID
		 *            the ID of that batch's votes
		 * @param requestId
		 *            the UUID of the timeout
		 * @param source
		 *            the address of the source node
		 * @param destination
		 *            the address of the destination node
		 */
		public VotingResponseMsg(boolean isConverged, int voteID, UUID requestId, Address source,
				Address destination) {
			super(requestId, source, destination);
			this.voteID = voteID;
			this.isConverged = isConverged;
		}

		/**
		 * Constructor to voting response message
		 * 
		 * @param vote
		 *            whether the vote is yes or no
		 * @param highestNode
		 *            the node that is farthest up the voters tree
		 * @param isConverged
		 *            whether the node has converged with its neighbors or not
		 * @param voteID
		 *            the ID of that batch's votes
		 * @param requestId
		 *            the UUID of the timeout
		 * @param source
		 *            the address of the source node
		 * @param destination
		 *            the address of the destination node
		 */
		public VotingResponseMsg(boolean vote, Address highestNode, boolean isConverged,
				int voteID, UUID requestId, Address source, Address destination) {
			super(requestId, source, destination);
			this.vote = vote;
			this.voteID = voteID;
			this.isConverged = isConverged;
			this.highestNode = highestNode;
		}

		/**
		 * Getter for the vote
		 * 
		 * @return true or false depending on whether the voter voted yes or no
		 */
		public boolean getVote() {
			return this.vote;
		}

		/**
		 * Setter for the vote
		 * 
		 * @param vote
		 *            true/false for yes/no note
		 */
		public void setVote(boolean vote) {
			this.vote = vote;
		}

		/**
		 * Getter for the vote id
		 * 
		 * @return the ID of that batch of votes
		 */
		public int getVoteID() {
			return this.voteID;
		}

		/**
		 * Getter for the boolean converged
		 * 
		 * @return true for the node being converged, otherwise false
		 */
		public boolean isConverged() {
			return this.isConverged;
		}

		/**
		 * Getter for the address of the highest node
		 * 
		 * @return the address of the highest node in the voter's view
		 */
		public Address getHighestNode() {
			return this.highestNode;
		}

		/**
		 * Setter for the address of the highest node in the voter's view
		 * 
		 * @param node
		 *            the address of the highest node
		 */
		public void setHighestNode(VodAddress node) {
			this.highestNode = node;
		}
	}

	/**
	 * Result of the leader vote AND heart beat message
	 */
	public static class VotingResultMsg extends ElectionMsg {
		private static final long serialVersionUID = -564040445303957299L;
		private Collection<Address> leaderView;

		/**
		 * Constructor for voting result message
		 * @param leaderView the view of the leader
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public VotingResultMsg(Collection<Address> leaderView, Address source, Address destination) {
			super(source, destination);
			this.leaderView = leaderView;
		}

		/**
		 * Getter for the view of the leader
		 * @return the leader's view
		 */
		public Collection<Address> getLeaderView() {
			return this.leaderView;
		}
	}

	/**
	 * Message a follower uses to ask the leader if it has been rejected
	 */
	public static class RejectFollowerMsg extends ElectionMsg {
		private static final long serialVersionUID = 461688333668274232L;

		/**
		 * Constructor for reject follower message
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public RejectFollowerMsg(Address source, Address destination) {
			super(source, destination);
		}
	}

	/**
	 * Message from leader to a follower used for confirming rejections
	 */
	public static class RejectFollowerConfirmationMsg extends ElectionMsg {
		private static final long serialVersionUID = -3554289448641957041L;
		private final boolean nodeIsInView;

		/**
		 * Constructor for reject follower confirmation message
		 * @param nodeIsInView whether the node is in the leader's view or not
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public RejectFollowerConfirmationMsg(boolean nodeIsInView, Address source,
				Address destination) {
			super(source, destination);
			this.nodeIsInView = nodeIsInView;
		}

		/**
		 * Getter for the node in view boolean
		 * @return true if the node is in the leader's view, otherwise false
		 */
		public boolean isNodeInView() {
			return this.nodeIsInView;
		}
	}

	/**
	 * Used by nodes who suspects the leader to be dead and wants to call for a
	 * vote
	 */
	public static class LeaderDeathMsg extends ElectionMsg {
		private static final long serialVersionUID = 4798983776527412760L;
		private Address leader;

		/**
		 * Constructor of leader death message
		 * @param leader the address of the dead leader
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public LeaderDeathMsg(Address leader, Address source, Address destination) {
			super(source, destination);
			this.leader = leader;
		}

		/**
		 * Getter for the address of the leader
		 * @return the address of the leader
		 */
		public Address getLeader() {
			return this.leader;
		}
	}

	/**
	 * Message containing information whether the node believes the leader to be
	 * dead or not
	 */
	public static class LeaderDeathResponseMsg extends ElectionMsg {
		private static final long serialVersionUID = 4798983776527412760L;
		private final boolean leaderIsAlive;
		private final Address leader;
		

		/**
		 * Constructor for leader death response message  
		 * @param leaderIsAlive true if the leader is alive
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public LeaderDeathResponseMsg(Address leader, boolean leaderIsAlive, Address source, Address destination) {
			super(source, destination);
			this.leader = leader;
			this.leaderIsAlive = leaderIsAlive;
		}

		/**
		 * Getter for the leader's address
		 * @return the address of the leader
		 */
		public Address getLeader() {
			return this.leader;
		}
		
		/**
		 * Getter for the is leader alive boolean
		 * @return true if the leader is alive
		 */
		public boolean isLeaderAlive() {
			return this.leaderIsAlive;
		}
	}

	/**
	 * Message containing the outcome from a leader death vote
	 */
	public static class TheLeaderIsDefinitelyConfirmedToBeDeadMsg extends ElectionMsg {
		private static final long serialVersionUID = 4798983776527412760L;
		private Address leader;

		/**
		 * Constructor for leader is definitely confirmed to be dead message
		 * @param leader the address to the dead leader
		 * @param source the address to the source node 
		 * @param destination the address to the destination node
		 */
		public TheLeaderIsDefinitelyConfirmedToBeDeadMsg(Address leader, Address source,
				Address destination) {
			super(source, destination);
			this.leader = leader;
		}

		/**
		 * Getter for the address of the leader
		 * @return the address of the leader
		 */
		public Address getLeader() {
			return this.leader;
		}
	}

	/**
	 * Message sent from a follower to a leader when a better leader has been
	 * found
	 */
	public static class RejectLeaderMsg extends ElectionMsg {
		private static final long serialVersionUID = -5233104766082888465L;
		private Address betterLeader;

		/**
		 * Constructor for rejected leader message
		 * @param betterLeader the address to a better suited leader
		 * @param source the address of the source node
		 * @param destination the address of the destination node
		 */
		public RejectLeaderMsg(Address betterLeader, Address source, Address destination) {
			super(source, destination);
			this.betterLeader = betterLeader;
		}

		public Address getBetterLeader() {
			return this.betterLeader;
		}
	}
}
