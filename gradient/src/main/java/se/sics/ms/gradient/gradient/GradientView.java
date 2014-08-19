package se.sics.ms.gradient.gradient;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.ms.gradient.misc.UtilityComparator;
import se.sics.ms.types.SearchDescriptor;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.types.PartitionId;
import se.sics.ms.util.PartitionHelper;


/**
 * Class representing the gradient view. It selects nodes according to the
 * preference function for a given node and offers functions to find the optimal
 * exchange partners for a given node.
 */
public class GradientView {
    private static final Logger logger = LoggerFactory.getLogger(GradientView.class);
    private TreeMap<VodAddress, SearchDescriptor> mapping;
	private TreeSet<SearchDescriptor> entries;
	private Self self;
	private int size;
	private final Comparator<SearchDescriptor> preferenceComparator;
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
        this.preferenceComparator = new PreferenceComparator(new SearchDescriptor(self.getDescriptor()));
        this.mapping = new TreeMap<VodAddress, SearchDescriptor>();
		this.entries = new TreeSet<SearchDescriptor>(utilityComparator);
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
	 * @param searchDescriptor
	 *            the searchDescriptor to be added
	 */
	protected void add(SearchDescriptor searchDescriptor) {
        if (searchDescriptor.getVodAddress().equals(self.getAddress())) {
            logger.warn("{} tried to add itself to its GradientView", self.getAddress());
            return;
        }

        int oldSize = entries.size();

        SearchDescriptor currDescriptor = null;

        for(SearchDescriptor descriptor : entries){
            if(descriptor.equals(searchDescriptor)){
                currDescriptor = descriptor;
                break;
            }
        }

        if(currDescriptor != null){
            if(currDescriptor.equals(searchDescriptor) && utilityComparator.compare(currDescriptor,searchDescriptor) ==0)
                return;
            else{
                entries.remove(currDescriptor);
                changed = true;
            }
        }



//        if (mapping.containsKey(searchDescriptor.getVodAddress())) {
//            SearchDescriptor currentSearchDescriptor = mapping.get(searchDescriptor.getVodAddress());
//            if (currentSearchDescriptor.equals(searchDescriptor) && utilityComparator.compare(currentSearchDescriptor, searchDescriptor) == 0) {
//                return;
//            } else {
//                entries.remove(currentSearchDescriptor);
//                changed = true;
//            }
//        }

//        mapping.put(searchDescriptor.getVodAddress(), searchDescriptor);
        entries.add(searchDescriptor);

        if (!changed) {
            changed = !(oldSize == entries.size());
        }

		if (entries.size() > size) {
			SortedSet<SearchDescriptor> set = getClosestNodes(size + 1);
            SearchDescriptor leastPreferred = set.first();
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
//        SearchDescriptor toRemove = mapping.remove(address);
        SearchDescriptor toRemove = null;

        for(SearchDescriptor descriptor:  entries){
            if(descriptor.getVodAddress().equals(address)){
                toRemove = descriptor;
                break;
            }
        }

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
	protected SearchDescriptor selectPeerToShuffleWith() {
		if (entries.isEmpty()) {
			return null;
		}

		incrementDescriptorAges();
//		SearchDescriptor oldestEntry = Collections.max(entries);
//
//		return oldestEntry;

      return getClosestNodes(1).first();
	}

	/**
	 * Merge a collection of nodes in the view and drop the least preferred
	 * nodes if the size limit is reached.
	 * 
	 * @param searchDescriptors
	 *            the nodes to be merged
	 */
	protected void merge(Collection<SearchDescriptor> searchDescriptors) {
        Collection<SearchDescriptor> oldEntries = (Collection<SearchDescriptor>) entries.clone();
		int oldSize = oldEntries.size();

        if(self.getAddress().getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {

            PartitionId myPartitionId = new PartitionId(self.getAddress().getPartitioningType(),
                    self.getAddress().getPartitionIdDepth(), self.getAddress().getPartitionId());
            PartitionHelper.adjustDescriptorsToNewPartitionId(myPartitionId, searchDescriptors);
        }

        for (SearchDescriptor searchDescriptor : searchDescriptors) {
            add(searchDescriptor);
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

    protected void adjustViewToNewPartitions() {
        PartitionId myPartitionId = new PartitionId(self.getAddress().getPartitioningType(),
                self.getAddress().getPartitionIdDepth(), self.getAddress().getPartitionId());
        PartitionHelper.adjustDescriptorsToNewPartitionId(myPartitionId, entries);
    }

	/**
	 * Return the number most preferred nodes for the given searchDescriptor.
	 * 
	 * @param searchDescriptor
	 *            the searchDescriptor to compare with
	 * @param number
	 *            the maximum number of entries to return
	 * @return a collection of the most preferred nodes
	 */
	protected SortedSet<SearchDescriptor> getExchangeDescriptors(SearchDescriptor searchDescriptor, int number) {
		SortedSet<SearchDescriptor> set = getClosestNodes(searchDescriptor, number);

        set.remove(searchDescriptor);

        try {
            assert !set.contains(searchDescriptor);
        } catch (AssertionError e) {
            StringBuilder builder = new StringBuilder();
            builder.append(self.getAddress().toString() + " should not include searchDescriptor of the exchange partner " + searchDescriptor.toString());
            builder.append("\n exchange set content:");
            for (SearchDescriptor a : set) {
                builder.append("\n" + a.toString());
            }
            AssertionError error = new AssertionError(builder);
            error.setStackTrace(e.getStackTrace());
            throw error;
        }

        //number - 1 because the source node will be later later
        while (set.size() > (number - 1)) {
            set.remove(set.first());
        }

        //as part of the protocol, the source node should also be added in the set, otherwise
        // message will be discarded on the receiving node
        set.add(new SearchDescriptor(self.getDescriptor()));

		return set;
	}

	/**
	 * @return all nodes with a higher preference value than self in ascending order
	 */
	protected SortedSet<SearchDescriptor> getHigherUtilityNodes() {
		return entries.tailSet(new SearchDescriptor(self.getDescriptor()));
	}

	/**
	 * @return all nodes with a lower preference value than self in ascending order
	 */
	protected SortedSet<SearchDescriptor> getLowerUtilityNodes() {
		return entries.headSet(new SearchDescriptor(self.getDescriptor()));
	}

	/**
	 * @return a list of all entries in the view
	 */
	protected SortedSet<SearchDescriptor> getAll() {
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
		for (SearchDescriptor node : entries) {
			builder.append(node.getVodAddress().getId() + " ");
		}
		return builder.toString();
	};
	
	/**
	 * Increment the age of all descriptors in the view
	 */
	private void incrementDescriptorAges() {
		for (SearchDescriptor descriptor : entries) {
			descriptor.incrementAndGetAge();
		}
	}

	/**
	 * Compare nodes according to their utility. Nodes with smaller IDs but
	 * closer to the base are the best once. Closer nodes are preferred to nodes
	 * further away.
	 */
	private class PreferenceComparator implements Comparator<SearchDescriptor> {
		private SearchDescriptor base;

		public PreferenceComparator(SearchDescriptor base) {
			super();
			this.base = base;
		}

		@Override
		public int compare(SearchDescriptor o1, SearchDescriptor o2) {

            if (utilityComparator.compare(o1, o2) == 0) {
                return 0;
            }

            if (utilityComparator.compare(o1, base) == 0) {
                return 1;
            }

            if (utilityComparator.compare(o2, base) == 0) {
                return -1;
            }

            if (utilityComparator.compare(o1, base) == 1 && utilityComparator.compare(o2, base) == -1) {
				return 1;
			}

            if (utilityComparator.compare(o1, base) == 1 && utilityComparator.compare(o2, base) == 1 && utilityComparator.compare(o1, o2) == 1) {
				return 1;
			}

            if (utilityComparator.compare(o1, base) == -1 && utilityComparator.compare(o2, base) == -1 && utilityComparator.compare(o1, o2) == 1) {
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
	private SortedSet<SearchDescriptor> getClosestNodes(int number) {
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
	private SortedSet<SearchDescriptor> getClosestNodes(SearchDescriptor address, int number) {
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
	private SortedSet<SearchDescriptor> getClosestNodes(int number, Comparator<SearchDescriptor> c) {
		SortedSet<SearchDescriptor> set = new TreeSet<SearchDescriptor>(c);
        set.addAll(getAll());
        while (set.size() > number) {
            set.remove(set.first());
        }
		return set;
	}
}
