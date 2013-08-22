package se.sics.ms.gradient;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;


/**
 * Class representing the gradient view. It selects nodes according to the
 * preference function for a given node and offers functions to find the optimal
 * exchange partners for a given node.
 */
public class GradientView {
    private static final Logger logger = LoggerFactory.getLogger(GradientView.class);
    private TreeMap<VodAddress, VodDescriptor> mapping;
	private TreeSet<VodDescriptor> entries;
	private Self self;
	private int size;
	private final Comparator<VodDescriptor> preferenceComparator;
    private final int convergenceTestRounds;
    private int currentConvergedRounds;
	private boolean converged, changed;
	private final double convergenceTest;
    private final UtilityComparator utilityComparator = new UtilityComparator();

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
        this.preferenceComparator = new PreferenceComparator(self.getDescriptor());
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
        if (vodDescriptor.getVodAddress().equals(self.getAddress())) {
            logger.warn("{} tried to add itself to its GradientView", self.getAddress());
            return;
        }

        int oldSize = entries.size();

        if (mapping.containsKey(vodDescriptor.getVodAddress())) {
            VodDescriptor currentVodDescriptor = mapping.get(vodDescriptor.getVodAddress());
            if (currentVodDescriptor.equals(vodDescriptor) && utilityComparator.compare(currentVodDescriptor, vodDescriptor) == 0) {
                return;
            } else {
                entries.remove(currentVodDescriptor);
                changed = true;
            }
        }

        mapping.put(vodDescriptor.getVodAddress(), vodDescriptor);
        entries.add(vodDescriptor);

        if (!changed) {
            changed = !(oldSize == entries.size());
        }

		if (entries.size() > size) {
			SortedSet<VodDescriptor> set = getClosestNodes(size + 1);
            VodDescriptor leastPreferred = set.first();
			remove(leastPreferred.getVodAddress());
		}
	}

    public void setChanged() {
        changed = true;
    }

	/**
	 * Remove a node from the view.
	 * 
	 * @param address
	 *            the node to be removed
	 */
	protected void remove(VodAddress address) {
        int oldSize = entries.size();
        VodDescriptor toRemove = mapping.remove(address);

        if (toRemove == null) {
            return;
        }

		entries.remove(toRemove);
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
	 * @param vodDescriptors
	 *            the nodes to be merged
	 */
	protected void merge(Collection<VodDescriptor> vodDescriptors) {
        Collection<VodDescriptor> oldEntries = (Collection<VodDescriptor>) entries.clone();
		int oldSize = oldEntries.size();

        boolean isOnePartition = ((MsSelfImpl)self).getPartitionsNumber() == 1;

		for (VodDescriptor vodDescriptor : vodDescriptors) {
            if(!isOnePartition) {
                int bitsToCheck = ((MsSelfImpl)self).getPartitionId().size();

                LinkedList<Boolean> partitionId = new LinkedList<Boolean>();

                for(int i=0; i<bitsToCheck; i++) {
                    partitionId.addFirst((vodDescriptor.getId() & (1<<i)) == 0);
                }

                vodDescriptor.setPartitionId(partitionId);
                vodDescriptor.setPartitionsNumber(((MsSelfImpl)self).getPartitionsNumber());
            }
            else {
                LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
                partitionId.addFirst(false);

                vodDescriptor.setPartitionId(partitionId);
                vodDescriptor.setPartitionsNumber(((MsSelfImpl)self).getPartitionsNumber());
            }

            if(vodDescriptor.getPartitionId().equals(((MsSelfImpl)self).getPartitionId()))
			    add(vodDescriptor);
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

    protected void adjustViewToNewPartitions(boolean isFirstSplit) {
//        if(!isFirstSplit) {
//            int bitsToCheck = ((MsSelfImpl)self).getPartitionId().size();
//
//            for(VodDescriptor descriptor : entries) {
//                LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
//
//                for(int i=0; i<bitsToCheck; i++) {
//                    partitionId.addFirst((descriptor.getId() & (1<<i)) == 0);
//                }
//
//                descriptor.setPartitionId(partitionId);
//                descriptor.setPartitionsNumber(((MsSelfImpl)self).getPartitionsNumber());
//            }
//        }
//        else {
//            for(VodDescriptor descriptor : entries) {
//                LinkedList<Boolean> partitionId = new LinkedList<Boolean>();
//                partitionId.addFirst(false);
//
//                descriptor.setPartitionId(partitionId);
//                descriptor.setPartitionsNumber(((MsSelfImpl)self).getPartitionsNumber());
//            }
//        }
//
//        Iterator<VodDescriptor> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            VodDescriptor next = iterator.next();
//            if(!next.getPartitionId().equals(((MsSelfImpl)self).getPartitionId()))  {
//                iterator.remove();
//            }
//        }


        int bitToCheck = ((MsSelfImpl)self).getPartitionId().size()-1;

        //calculate partitionIds
        for(VodDescriptor descriptor : entries) {
            int nodeId = descriptor.getId();

            boolean partition = (nodeId & (1 << bitToCheck)) == 0;

            if(isFirstSplit) {
                LinkedList<Boolean> partitionId  = new LinkedList<Boolean>();
                partitionId.addFirst(partition);
                descriptor.setPartitionId(partitionId);
                descriptor.setPartitionsNumber(2);
            }
            else {
                LinkedList<Boolean> partitionId = descriptor.getPartitionId();
                partitionId.addFirst(partition);
                descriptor.setPartitionsNumber(descriptor.getPartitionsNumber()+1);
            }
        }

        //remove all peers not from your overlay
        VodDescriptor[] temp = entries.toArray(new VodDescriptor[entries.size()]);
        for(VodDescriptor descriptor : temp) {
            if(!descriptor.getPartitionId().equals(((MsSelfImpl)self).getPartitionId())) {
                entries.remove(descriptor);
            }
        }
    }

	/**
	 * Return the number most preferred nodes for the given vodDescriptor.
	 * 
	 * @param vodDescriptor
	 *            the vodDescriptor to compare with
	 * @param number
	 *            the maximum number of entries to return
	 * @return a collection of the most preferred nodes
	 */
	protected SortedSet<VodDescriptor> getExchangeDescriptors(VodDescriptor vodDescriptor, int number) {
		SortedSet<VodDescriptor> set = getClosestNodes(vodDescriptor, number);

        set.add(self.getDescriptor());
        set.remove(vodDescriptor);

        try {
            assert !set.contains(vodDescriptor);
        } catch (AssertionError e) {
            StringBuilder builder = new StringBuilder();
            builder.append(self.getAddress().toString() + " should not include vodDescriptor of the exchange partner " + vodDescriptor.toString());
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
		return entries.tailSet(self.getDescriptor());
	}

	/**
	 * @return all nodes with a lower preference value than self in ascending order
	 */
	protected SortedSet<VodDescriptor> getLowerUtilityNodes() {
		return entries.headSet(self.getDescriptor());
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
	private class PreferenceComparator implements Comparator<VodDescriptor> {
		private VodDescriptor base;

		public PreferenceComparator(VodDescriptor base) {
			super();
			this.base = base;
		}

		@Override
		public int compare(VodDescriptor o1, VodDescriptor o2) {

            if (utilityComparator.compare(o1, o2) == 0) {
                return 0;
            } else if (utilityComparator.compare(o1, base) == 0) {
                return 1;
            } else if (utilityComparator.compare(o2, base) == 0) {
                return -1;
            } else if (utilityComparator.compare(o1, base) == 1 && utilityComparator.compare(o2, base) == -1) {
				return 1;
			} else if (utilityComparator.compare(o1, base) == 1 && utilityComparator.compare(o2, base) == 1 && utilityComparator.compare(o1, o2) == 1) {
				return 1;
			} else if (utilityComparator.compare(o1, base) == -1 && utilityComparator.compare(o2, base) == -1 && utilityComparator.compare(o1, o2) == 1) {
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
		return getClosestNodes(number, preferenceComparator);
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
		return getClosestNodes(number, new PreferenceComparator(address));
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
