package se.sics.ms.gradient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;


/**
 * Class representing the gradient view. It selects nodes according to the
 * preference function for a given node and offers functions to find the optimal
 * exchange partners for a given node.
 */
public class GradientView {
	private TreeMap<VodAddress, VodDescriptor> entries;
	private Self self;
	private int size;
	private Comparator<VodAddress> closerComparator;
	private boolean converged;
	private final double convergenceSimilarity;

	/**
	 * @param self
	 *            the address of the local node
	 * @param size
	 *            the maximum size of this view
	 * @param convergenceSimilarity
	 *            the percentage of nodes allowed to change in order to be
	 *            converged
	 */
	public GradientView(Self self, int size, double convergenceSimilarity) {
		this.entries = new TreeMap<VodAddress, VodDescriptor>();
		this.closerComparator = new Closer(self.getAddress());
		this.self = self;
		this.size = size;
		this.converged = false;
		this.convergenceSimilarity = convergenceSimilarity;
	}

	/**
	 * Add a new node to the view and drop the least preferred one if the view
	 * is full.
	 * 
	 * @param address
	 *            the node to be added
	 */
	public void add(VodAddress address) {
		entries.put(address, new VodDescriptor(address));

		if (entries.size() > size) {
			List<VodAddress> list = getClosestNodes(size);
			VodAddress leastPreferred = list.get(0);
			remove(leastPreferred);
		}
	}

	/**
	 * Remove a node from the view.
	 * 
	 * @param address
	 *            the node to be removed
	 */
	public void remove(VodAddress address) {
		entries.remove(address);
	}

	/**
	 * Return the node with the oldest age.
	 * 
	 * @return the address of the node with the oldest age
	 */
	public VodAddress selectPeerToShuffleWith() {
		if (entries.isEmpty()) {
			return null;
		}

		incrementDescriptorAges();
		VodDescriptor oldestEntry = Collections.max(entries.values());

		return oldestEntry.getVodAddress();
	}

	/**
	 * Merge a collection of nodes in the view and drop the least preferred
	 * nodes if the size limit is reached.
	 * 
	 * @param addresses
	 *            the nodes to be merged
	 */
	public void merge(VodAddress[] addresses) {
		Collection<VodAddress> old = new ArrayList<VodAddress>(entries.keySet());
		int oldSize = old.size();

		for (VodAddress address : addresses) {
			add(address);
		}

		old.retainAll(entries.keySet());
		if (oldSize == entries.size() && old.size() > convergenceSimilarity * entries.size()) {
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
	public Collection<VodAddress> getExchangeNodes(VodAddress address, int number) {
		List<VodAddress> list = getClosestNodes(address, number);
		list.add(self.getAddress());
		Collections.sort(list, new Closer(address));
		list.remove(address);
		return list.subList(0, number < list.size() ? number : list.size());
	}

	/**
	 * @return all nodes with a higher preference value than self
	 */
	public ArrayList<VodAddress> getHigherNodes() {
		return new ArrayList<VodAddress>(entries.headMap(self.getAddress()).keySet());
	}

	/**
	 * @return all nodes with a lower preference value than self
	 */
	public ArrayList<VodAddress> getLowerNodes() {
		return new ArrayList<VodAddress>(entries.tailMap(self.getAddress()).keySet());
	}

	/**
	 * @return a list of all entries in the view
	 */
	public ArrayList<VodAddress> getAll() {
		return new ArrayList<VodAddress>(entries.keySet());
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (VodAddress node : entries.keySet()) {
			builder.append(node.getId() + " ");
		}
		return builder.toString();
	};
	
	/**
	 * Increment the age of all descriptors in the view
	 */
	private void incrementDescriptorAges() {
		for (VodDescriptor descriptor : entries.values()) {
			descriptor.incrementAndGetAge();
		}
	}

	/**
	 * Compare nodes according to their utility. Nodes with smaller IDs but
	 * closer to the base are the best once. Closer nodes are preferred to nodes
	 * further away.
	 */
	private class Closer implements Comparator<VodAddress> {
		private VodAddress base;

		public Closer(VodAddress base) {
			super();
			this.base = base;
		}

		@Override
		public int compare(VodAddress o1, VodAddress o2) {
			assert (o1.getId() == o2.getId());

			if (o1.getId() < base.getId() && o2.getId() > base.getId()) {
				return 1;
			} else if (o1.getId() < base.getId() && o2.getId() < base.getId()
					&& o1.getId() > o2.getId()) {
				return 1;
			} else if (o1.getId() > base.getId() && o2.getId() > base.getId()
					&& o1.getId() < o2.getId()) {
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
	 * @return the list of the closest nodes to self
	 */
	private List<VodAddress> getClosestNodes(int number) {
		return getClosestNodes(self.getAddress(), number, closerComparator);
	}

	/**
	 * Get a sorted list of the nodes that are the closest to the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of nodes to return
	 * @return the list of the closest nodes to the given address
	 */
	private List<VodAddress> getClosestNodes(VodAddress address, int number) {
		return getClosestNodes(address, number, new Closer(address));
	}

	/**
	 * Get a sorted list of the nodes that are the closest to the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of nodes to return
	 * @param c
	 *            the comparator to use
	 * @return the list of the closest nodes to the given address
	 */
	private List<VodAddress> getClosestNodes(VodAddress address, int number, Comparator<VodAddress> c) {
		ArrayList<VodAddress> addresses = getAll();
		Collections.sort(addresses, new Closer(address));
		return addresses.subList(0, number < addresses.size() ? number : addresses.size());
	}
}
