package se.sics.ms.gradient;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;


/**
 * Class representing the gradient view. It selects nodes according to the
 * preference function for a given node and offers functions to find the optimal
 * exchange partners for a given node.
 */
public class GradientView {
    private static final Logger logger = LoggerFactory.getLogger(GradientView.class);
	private TreeSet<VodDescriptor> entries;
    private TreeMap<VodAddress, VodDescriptor> mapping;
	private Self self;
	private int size;
	private final Comparator<VodDescriptor> utilityComparator;
    private final int convergenceTestRounds;
    private int currentConvergedRounds;
	private boolean converged, changed;
	private final double convergenceTest;

	/**
	 * @param self
	 *            the address of the local node
	 * @param size
	 *            the maximum size of this view
	 * @param convergenceTest
	 *            the percentage of nodes allowed to change in order to be
	 *            converged
     * @param convergenceTestRounds
     *            the number of rounds the convergenceTest needs to be satisfied for the view to be converged
	 */
	public GradientView(Self self, int size, double convergenceTest, int convergenceTestRounds) {
        this.utilityComparator = new UtilityComparator(self.getDescriptor());
        this.mapping = new TreeMap<VodAddress, VodDescriptor>();
		this.entries = new TreeSet<VodDescriptor>(utilityComparator);
		this.self = self;
		this.size = size;
		this.converged = false;
        this.changed = false;
		this.convergenceTest = convergenceTest;
        this.convergenceTestRounds = convergenceTestRounds;
	}

	/**
	 * Add a new node to the view and drop the least preferred one if the view
	 * is full.
	 * 
	 * @param vodDescriptor
	 *            the vodDescriptor to be added
	 */
	protected void add(VodDescriptor vodDescriptor) {
        if (vodDescriptor.equals(self.getDescriptor())) {
            logger.warn("{} tried to add itself to its GradientView", self.getAddress());
            return;
        }

        int oldSize = entries.size();
		entries.add(vodDescriptor);
        mapping.put(vodDescriptor.getVodAddress(), vodDescriptor);

        if (!changed) {
            changed = !(oldSize == entries.size());
        }

		if (entries.size() > size) {
			SortedSet<VodDescriptor> set = getClosestNodes(size);
            VodDescriptor leastPreferred = set.first();
			remove(leastPreferred);
		}
	}

	/**
	 * Remove a node from the view.
	 * 
	 * @param address
	 *            the node to be removed
	 */
	protected void remove(VodDescriptor address) {
        int oldSize = entries.size();
		entries.remove(address);
        if (!changed) {
            changed = !(oldSize == entries.size());
        }
	}

	/**
	 * Return the node with the oldest age.
	 * 
	 * @return the address of the node with the oldest age
	 */
	protected VodDescriptor selectPeerToShuffleWith() {
		if (entries.isEmpty()) {
			return null;
		}

		incrementDescriptorAges();
		VodDescriptor oldestEntry = Collections.max(entries);

		return oldestEntry;
	}

	/**
	 * Merge a collection of nodes in the view and drop the least preferred
	 * nodes if the size limit is reached.
	 * 
	 * @param addresses
	 *            the nodes to be merged
	 */
	protected void merge(VodDescriptor[] addresses) {
        Collection<VodDescriptor> oldEntries = (Collection<VodDescriptor>) entries.clone();
		int oldSize = oldEntries.size();

		for (VodDescriptor address : addresses) {
			add(address);
		}

		oldEntries.retainAll(entries);
		if (oldSize == entries.size() && oldEntries.size() > convergenceTest * entries.size()) {
            currentConvergedRounds++;
		} else {
            currentConvergedRounds = 0;
		}
        if (currentConvergedRounds > convergenceTestRounds) {
            if (!converged) {
                this.changed = true;
            }
            converged = true;
        } else {
            converged = false;
        }
	}

	/**
	 * Return the number most preferred nodes for the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of entries to return
	 * @return a collection of the most preferred nodes
	 */
	protected SortedSet<VodDescriptor> getExchangeNodes(VodDescriptor address, int number) {
		SortedSet<VodDescriptor> set = getClosestNodes(address, number);
		set.add(self.getDescriptor());
        set.remove(address);

        try {
            assert !set.contains(address);
        } catch (AssertionError e) {
            StringBuilder builder = new StringBuilder();
            builder.append(self.getAddress().toString() + " should not include address of the exchange partner " + address.toString());
            builder.append("\n exchange set content:");
            for (VodDescriptor a : set) {
                builder.append("\n" + a.toString());
            }
            AssertionError error = new AssertionError(builder);
            error.setStackTrace(e.getStackTrace());
            throw error;
        }

        while (set.size() > number) {
            set.remove(set.first());
        }

		return set;
	}

	/**
	 * @return all nodes with a higher preference value than self in ascending order
	 */
	protected SortedSet<VodDescriptor> getHigherUtilityNodes() {
		return entries.headSet(self.getDescriptor());
	}

	/**
	 * @return all nodes with a lower preference value than self in ascending order
	 */
	protected SortedSet<VodDescriptor> getLowerUtilityNodes() {
		return entries.tailSet(self.getDescriptor());
	}

	/**
	 * @return a list of all entries in the view
	 */
	protected SortedSet<VodDescriptor> getAll() {
		return entries;
	}

	/**
	 * @return true if the view is empty
	 */
	public boolean isEmpty() {
		return entries.isEmpty();
	}

	/**
	 * @return the size of the view
	 */
	public int getSize() {
		return entries.size();
	}

	/**
	 * @return true if the convergence criteria are reached for this view
	 */
	public boolean isConverged() {
		return converged;
	}

	/**
	 * @return true if the size limit was reached
	 */
	public boolean isFull() {
		return this.size <= entries.size();
	}

    public boolean isChanged() {
        return changed;
    }

    @Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (VodDescriptor node : entries) {
			builder.append(node.getVodAddress().getId() + " ");
		}
		return builder.toString();
	};
	
	/**
	 * Increment the age of all descriptors in the view
	 */
	private void incrementDescriptorAges() {
		for (VodDescriptor descriptor : entries) {
			descriptor.incrementAndGetAge();
		}
	}

	/**
	 * Compare nodes according to their utility. Nodes with smaller IDs but
	 * closer to the base are the best once. Closer nodes are preferred to nodes
	 * further away.
	 */
	private class UtilityComparator implements Comparator<VodDescriptor> {
		private VodDescriptor base;

		public UtilityComparator(VodDescriptor base) {
			super();
			this.base = base;
		}

		@Override
		public int compare(VodDescriptor o1, VodDescriptor o2) {
            int baseId = base.getVodAddress().getId();
            int id1 = o1.getVodAddress().getId();
            int id2 = o2.getVodAddress().getId();
			try {
                assert id1 != id2;
            } catch (AssertionError e) {
                StringBuilder builder = new StringBuilder();
                builder.append(self.getAddress().toString() + " duplicated view entries are forbidden\n");
                builder.append("View content:");
                for (VodDescriptor a : entries) {
                    builder.append("\n" + a.toString());
                }
                AssertionError error = new AssertionError(builder);
                error.setStackTrace(e.getStackTrace());
                throw error;
            }

            if (id1 == baseId) {
                return 1;
            } else if (id2 == baseId) {
                return -1;
            } else if (id1 < baseId && id2 > baseId) {
				return 1;
			} else if (id1 < baseId && id2 < baseId && id1 > id2) {
				return 1;
			} else if (id1 > baseId && id2 > baseId && id1 < id2) {
				return 1;
			}
			return -1;
		}
	}
	
	/**
	 * Get a sorted list of the nodes that are the closest to self.
	 * 
	 * @param number
	 *            the maximum number of nodes to return
	 * @return a sorted list of the closest nodes to self
	 */
	private SortedSet<VodDescriptor> getClosestNodes(int number) {
		return getClosestNodes(number, utilityComparator);
	}

	/**
	 * Get a sorted list of the nodes that are the closest to the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of nodes to return
	 * @return a sorted list of the closest nodes to the given address
	 */
	private SortedSet<VodDescriptor> getClosestNodes(VodDescriptor address, int number) {
		return getClosestNodes(number, new UtilityComparator(address));
	}

	/**
	 * Get a sorted set of the nodes that are the closest to the given address.
	 *
	 * @param number
	 *            the maximum number of nodes to return
	 * @param c the comparator used for sorting
	 *            the comparator to use
	 * @return a sorted list of the closest nodes to the given address
	 */
	private SortedSet<VodDescriptor> getClosestNodes(int number, Comparator<VodDescriptor> c) {
		SortedSet<VodDescriptor> set = new TreeSet<VodDescriptor>(c);
        set.addAll(getAll());
        while (set.size() > number) {
            set.remove(set.first());
        }
		return set;
	}
}
