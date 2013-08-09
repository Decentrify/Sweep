package se.sics.ms.gradient;

import java.util.ArrayList;
import java.util.SortedSet;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;

/**
 * This class is a port created for the purpose of broadcasting gradient's view
 */
public class GradientViewChangePort extends PortType {
	{
		negative(GradientViewChanged.class);
	}

	/**
	 * An event that contains the Gradient's view
	 */
	public static class GradientViewChanged extends Event {
		private final boolean isConverged;
		private final SortedSet<VodDescriptor> gradientView;

		/**
		 * Default constructor
		 * 
		 * @param isConverged
		 *            true if the node's view has converged
		 * @param gradientView
		 *            the current view of the gradient
		 */
		public GradientViewChanged(boolean isConverged, SortedSet<VodDescriptor> gradientView) {
			super();
			this.isConverged = isConverged;
			this.gradientView = gradientView;
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
         * The set is backed by the view of this event so changes to one of them affects the other
         *
         * @return all nodes in the view
         */
        public SortedSet<VodDescriptor> getGradientView() {
            return gradientView;
        }

        /**
         * The set is backed by the view of this event so changes to one of them affects the other.
         *
         * @return all nodes with a higher preference value than the given descriptor in ascending order
         */
        public SortedSet<VodDescriptor> getHigherUtilityNodes(VodDescriptor vodDescriptor) {
            return gradientView.tailSet(vodDescriptor);
        }

        /**
         * The set is backed by the view of this event so changes to one of them affects the other
         *
         * @return all nodes with a lower preference value than the given descriptor in ascending order
         */
        public SortedSet<VodDescriptor> getLowerUtilityNodes(VodDescriptor vodDescriptor) {
            return gradientView.headSet(vodDescriptor);
        }
	}
}
