package se.sics.ms.gradient;

import java.util.ArrayList;

import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;

/**
 * This class is a port created for the purpose of broadcasting TMan's view
 */
public class BroadcastGradientPartnersPort extends PortType {
	{
		negative(TmanPartners.class);
	}

	/**
	 * An event that contains TMan's view
	 */
	public static class TmanPartners extends Event {
		private final boolean isConverged;
		private final ArrayList<VodAddress> higherNodes, lowerNodes;

		/**
		 * Default constructor
		 * 
		 * @param isConverged
		 *            true if the node's view has converged
		 * @param higherNodes
		 *            a list of nodes with higher utility values compared to
		 *            itself
		 * @param lowerNodes
		 *            a list of nodes with lower utility values comepared to
		 *            itself
		 */
		public TmanPartners(boolean isConverged, ArrayList<VodAddress> higherNodes,
				ArrayList<VodAddress> lowerNodes) {
			super();
			this.isConverged = isConverged;
			this.higherNodes = higherNodes;
			this.lowerNodes = lowerNodes;
		}

		/**
		 * Getter for converged
		 * 
		 * @return true if the view has converged
		 */
		public boolean isConverged() {
			return this.isConverged;
		}

		/**
		 * Getter for the list of nodes with higher utility values than itself
		 * 
		 * @return the list of nodes
		 */
		public ArrayList<VodAddress> getHigherNodes() {
			return this.higherNodes;
		}

		/**
		 * Getter for the list of nodes with lower utility values than itself
		 * 
		 * @return the list of nodes
		 */
		public ArrayList<VodAddress> getLowerNodes() {
			return this.lowerNodes;
		}
	}
}
